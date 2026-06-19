# Scripts Reference

All scripts live in `scripts/`. Run from the **repo root** with the virtualenv active:

```bash
source .venv/bin/activate
```

---

## Quick-copy: full demo run order

```bash
# ── ONCE: import connection JSON → .env + SSL cert ───────────────────────────
python scripts/prepare_watsonx_env.py
# then open .env and set WXD_API_KEY=<your-key>

# ── EVERY SESSION: validate auth ─────────────────────────────────────────────
python scripts/get_token.py --export     # checks key, writes bearer token to .env

# ── ONCE on fresh env: create schemas ────────────────────────────────────────
python scripts/bootstrap_watsonxdata.py

# ── PATH A · dbt ─────────────────────────────────────────────────────────────
bash scripts/dbt_env.sh seed
bash scripts/dbt_env.sh run
bash scripts/dbt_env.sh test

# ── PATH B · Spark ───────────────────────────────────────────────────────────
# terminal 2 (keep open while uploading):
oc -n cpd-instance port-forward svc/ibm-lh-lakehouse-minio-svc 19000:9000

python scripts/upload_spark_assets.py
WXD_SPARK_DRY_RUN=false python scripts/submit_spark_application.py
python scripts/spark_application_status.py   # repeat until finished

# ── PATH C · cpdctl ingestion ────────────────────────────────────────────────
python scripts/ingest_with_cpdctl.py --wait          # submit + poll to completion
python scripts/ingest_with_cpdctl.py --status --batch <id>   # check a prior run

# ── QUERY gold layer ─────────────────────────────────────────────────────────
python scripts/query_gold.py

# ── OPTIONAL: Iceberg time travel demo ───────────────────────────────────────
python scripts/demo_time_travel.py

# ── OPTIONAL: OpenMetadata artifacts ─────────────────────────────────────────
python scripts/prepare_openmetadata_dbt_artifacts.py
python scripts/upload_dbt_artifacts.py

# ── TEARDOWN (destructive!) ───────────────────────────────────────────────────
python scripts/cleanup_watsonxdata.py
```

---

## Step-by-step reference

### 0a · `prepare_watsonx_env.py` — import connection JSON

**Run once** after downloading the Presto connection JSON from the watsonx.data console
(**Engine details → Download connection**). Save the file as `watsonx_data/instance_details.json`, then:

```bash
python scripts/prepare_watsonx_env.py
```

Writes `WXD_HOST`, `WXD_PORT`, `WXD_INSTANCE_ID`, `WXD_CPD_HOST`, `WXD_CPD_AUTH_URL`,
and the SSL cert to `certs/watsonxdata-ca.pem`. Preserves any existing `WXD_API_KEY`.

| Flag | Default | Description |
|---|---|---|
| `--connection-json PATH` | `watsonx_data/instance_details.json` | Downloaded JSON |
| `--env-file PATH` | `.env` | Target env file |
| `--cert-file PATH` | `certs/watsonxdata-ca.pem` | PEM output path |
| `--overwrite` | off | Re-import non-secret values even if already set |

---

### 0b · `get_token.py` — validate auth + refresh API key

**Run at the start of every session.** Use `--export` so the bearer token is written to `.env`
before running Spark (Spark submission reads `WXD_SPARK_BEARER_TOKEN`).

```bash
python scripts/get_token.py --export
```

If the API key is expired the script falls back to password login, regenerates a fresh key,
saves it to `.env`, and continues — no manual copy-paste needed.

```bash
# Force password login + regenerate key (e.g. after a long break)
python scripts/get_token.py --refresh-key --export
```

| Flag | Description |
|---|---|
| `--export` | Write bearer token to `WXD_SPARK_BEARER_TOKEN` in `.env` |
| `--refresh-key` | Skip API key check, login with password, regenerate and save new key |
| `--env-file PATH` | Override `.env` path |

??? tip "Getting an API key from the UI"
    1. Open `https://<WXD_CPD_HOST>` and log in as `cpadmin`.
    2. Avatar (top-right) → **Profile and settings** → **API key** tab.
    3. **Regenerate API key** → copy the value.
    4. Paste into `.env`: `WXD_API_KEY=<new-key>`.

---

### 1 · `bootstrap_watsonxdata.py` — create demo schemas

**Run once on a fresh environment**, before dbt or Spark.

```bash
python scripts/bootstrap_watsonxdata.py
```

Creates `lakehouse_demo_raw`, `lakehouse_demo_bronze`, `lakehouse_demo_silver`,
`lakehouse_demo_gold` in the `iceberg_data` catalog.

---

### 2 · `dbt_env.sh` — dbt with `.env` loaded

Wrapper that sources `.env` before calling dbt — use this instead of calling `dbt` directly.

```bash
bash scripts/dbt_env.sh seed              # load CSV seeds → raw tables
bash scripts/dbt_env.sh run               # build bronze → silver → gold
bash scripts/dbt_env.sh test              # run data quality tests

bash scripts/dbt_env.sh run --select silver_orders   # single model
bash scripts/dbt_env.sh run --full-refresh            # force full rebuild
bash scripts/dbt_env.sh docs generate && bash scripts/dbt_env.sh docs serve
```

---

### 3 · Spark path

#### `upload_spark_assets.py` — push app + CSVs to MinIO

Run **once** before submitting the Spark job. MinIO has no external route,
so open a port-forward first (keep it open in a second terminal):

```bash
oc -n cpd-instance port-forward svc/ibm-lh-lakehouse-minio-svc 19000:9000
```

```bash
python scripts/upload_spark_assets.py
```

Uploads `spark/app/load_medallion_demo.py` and all seed CSVs to `s3a://iceberg-bucket/spark_demo/`.

---

#### `submit_spark_application.py` — submit the Spark job

Requires `WXD_SPARK_BEARER_TOKEN` in `.env` — run `get_token.py --export` first.

```bash
# Dry run (default) — prints request body, does not submit
python scripts/submit_spark_application.py

# Real submission
WXD_SPARK_DRY_RUN=false python scripts/submit_spark_application.py
```

Prints the application ID on success. Copy it to `.env` as `WXD_SPARK_APPLICATION_ID`.

---

#### `spark_application_status.py` — check job status

```bash
python scripts/spark_application_status.py               # reads WXD_SPARK_APPLICATION_ID
python scripts/spark_application_status.py <app-id>      # or pass directly
```

Returns `running`, `finished`, or `failed`.

---

### 4 · `ingest_with_cpdctl.py` — native CSV ingestion

Requires `cpdctl` installed and on `PATH`. You do **not** need to log cpdctl in by
hand: every run validates `WXD_API_KEY` against CPD and re-syncs cpdctl's cached
credentials from `.env` first, so the stale-cache
`authenticate step: Unauthorized` error can't recur. If the key itself is rejected
you get a clear 401 pointing you to `python scripts/get_token.py --refresh-key`.

```bash
python scripts/ingest_with_cpdctl.py                  # submit all four jobs
python scripts/ingest_with_cpdctl.py --wait           # submit, then poll to completion
python scripts/ingest_with_cpdctl.py --status --batch <id>         # check a prior run
python scripts/ingest_with_cpdctl.py --status --batch <id> --wait  # poll a prior run
```

Reads CSVs from `s3a://iceberg-bucket/spark_demo/raw/` and ingests them into
`lakehouse_demo_ingest` via the watsonx.data native ingestion engine.

| Flag | Default | What it does |
|------|---------|--------------|
| `--wait` | off | After submitting, poll all jobs until they reach a terminal state. |
| `--status` | off | Skip submission; just report the status of an existing batch's jobs. |
| `--batch <id>` | timestamp | Batch id to target. Each run prints its id; pass it back to `--status`. |
| `--interval <s>` | `20` | Seconds between polls when waiting. |
| `--timeout <s>` | `900` | Max seconds to wait before giving up. |

Job ids are deterministic (`ingest-<table>-<batch>`), so `--status` needs only the
batch id — no state file. Status is read via `cpdctl wx-data ingestion get`.

---

### 5 · `query_gold.py` — query the gold layer

```bash
python scripts/query_gold.py              # all reports
python scripts/query_gold.py daily_sales
python scripts/query_gold.py customer_360
```

Connects to Presto and prints formatted tables. Requires a valid API key in `.env`
(run `get_token.py --export` first if you haven't this session).

---

### 6 · `demo_time_travel.py` — Iceberg time travel

```bash
python scripts/demo_time_travel.py
```

Shows snapshot history, partition metadata, and a time-travel query against the silver layer.
Requires the silver schema to exist (run dbt or Spark path first).

---

### 7 · OpenMetadata artifacts

#### `prepare_openmetadata_dbt_artifacts.py`

```bash
python scripts/prepare_openmetadata_dbt_artifacts.py            # full run: seed+run+test+docs
python scripts/prepare_openmetadata_dbt_artifacts.py --skip-dbt  # use existing target/*.json
python scripts/prepare_openmetadata_dbt_artifacts.py --skip-seed # skip seed step only
```

Stages `manifest.json`, `catalog.json`, and `run_results.json` into `openmetadata/dbt-artifacts/`.

---

#### `upload_dbt_artifacts.py`

```bash
python scripts/upload_dbt_artifacts.py
```

Pushes staged artifacts to `s3://iceberg-bucket/openmetadata/dbt-artifacts/lakehouse_demo/`
so OpenMetadata can read them during ingestion.

---

### 8 · `cleanup_watsonxdata.py` — tear down

```bash
python scripts/cleanup_watsonxdata.py
```

!!! danger "Destructive"
    Drops **all** `lakehouse_demo_*` and `spark_demo_*` schemas and every table inside them.
    Only use when fully tearing down the demo environment.
