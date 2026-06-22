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
scripts/reset_demo.sh --all --dry-run   # preview everything that would be removed
scripts/reset_demo.sh --all             # Docker stacks + schemas + MinIO files
# (or just the schemas, like before:)
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

Creates `dbt_demo_raw`, `dbt_demo_bronze`, `dbt_demo_silver`,
`dbt_demo_gold` in the `iceberg_data` catalog.

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
`spark_demo_cpdctl_raw` via the watsonx.data native ingestion engine.

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
python scripts/prepare_openmetadata_dbt_artifacts.py --docs-only # lineage only: docs generate + stage
python scripts/prepare_openmetadata_dbt_artifacts.py --skip-dbt  # use existing target/*.json
python scripts/prepare_openmetadata_dbt_artifacts.py --skip-seed # skip seed step only
```

By default the script runs `dbt seed --full-refresh`, `dbt run`, `dbt test`, and
`dbt docs generate`, then stages `manifest.json`, `catalog.json`, and `run_results.json`
into `openmetadata/dbt-artifacts/`.

| Flag | Default | What it does |
|------|---------|--------------|
| `--docs-only` | off | Lineage-only mode: run **only** `dbt docs generate` (no seed/run/test), then stage. Requires the medallion tables to already exist. Mutually exclusive with `--skip-dbt`. |
| `--skip-dbt` | off | Only copy existing `target/*.json` artifacts; do not run any dbt commands. |
| `--skip-seed` | off | Skip the `dbt seed` step but still run `dbt run`, `dbt test`, and `dbt docs generate`. |
| `--artifact-dir <path>` | `openmetadata/dbt-artifacts/` | Directory where the staged artifacts are written. |
| `--retries <n>` | `1` | Number of retries for each dbt command. |

---

#### `generate_lineage_docs.sh`

```bash
scripts/generate_lineage_docs.sh                 # refresh lineage artifacts only
scripts/generate_lineage_docs.sh --retries 3     # flags forwarded to the python stager
```

A thin, lineage-only convenience wrapper around
`prepare_openmetadata_dbt_artifacts.py --docs-only`. It runs **only**
`dbt docs generate` (no seed/run/test) and stages the three artifacts OpenMetadata
reads. Use it when the medallion tables are already built and you just want fresh
lineage/column metadata for the catalogue. All flags are forwarded verbatim to the
python stager (e.g. `--artifact-dir`, `--retries`); do not pass `--skip-dbt`.

---

#### `upload_dbt_artifacts.py`

```bash
python scripts/upload_dbt_artifacts.py
```

Pushes staged artifacts to `s3://iceberg-bucket/openmetadata/dbt-artifacts/dbt_demo/`
so OpenMetadata can read them during ingestion.

#### `apply_openmetadata_governance.py`

```bash
python scripts/apply_openmetadata_governance.py --mode online
python scripts/apply_openmetadata_governance.py --mode offline --strict
```

Applies the OpenMetadata governance layer after table and dbt lineage ingestion:
`MedallionGlossary`, `MedallionLayer`, `DemoDataDomain`, and
`MetadataIngestionMode`. The main ingestion script runs it automatically. Use this
manual command only when you want to re-apply glossary terms, descriptions, or
auto-classification tags without re-running the full ingestion.

---

### 8 · `cleanup_watsonxdata.py` — drop the schemas

```bash
python scripts/cleanup_watsonxdata.py
```

Drops every demo schema and the tables/views inside them: `dbt_demo_{raw,bronze,silver,gold}`,
`spark_demo_{bronze,silver,gold}`, and the cpdctl raw schema (`WXD_INGEST_SCHEMA`,
default `spark_demo_cpdctl_raw`).

!!! danger "Destructive"
    Drops the catalog objects only. Iceberg **data files** may linger in object storage —
    use `cleanup_minio.py` (or the all-in-one `reset_demo.sh` below) to delete those too.

### 9 · `cleanup_minio.py` — delete the demo's MinIO files

```bash
python scripts/cleanup_minio.py --dry-run   # list what would be deleted
python scripts/cleanup_minio.py             # delete
```

Scoped deletion of only the demo's own prefixes inside `iceberg-bucket`: the medallion
schema folders (Iceberg table data at the bucket root), the `spark_demo/` asset prefix
(uploaded Spark app + raw CSVs), and `openmetadata/dbt-artifacts/`. It never empties the
whole bucket. Needs an `oc` session (MinIO has no external Route on this cluster).

### 10 · `reset_demo.sh` — full reset for a 100% clean rerun

One command that resets any combination of the three demo "surfaces":

```bash
scripts/reset_demo.sh --all --dry-run     # preview a full wipe (changes nothing)
scripts/reset_demo.sh --all               # Docker + schemas + MinIO
scripts/reset_demo.sh --docker            # just the local containers/volumes/images
scripts/reset_demo.sh --warehouse -y      # drop schemas + MinIO files, skip the prompt
```

| Flag | What it resets |
|---|---|
| `--docker` | Stops & removes the Metabase, Airflow, OpenMetadata stacks — containers + named volumes + the demo's **own (locally built)** images. Shared public base images are kept by default. |
| `--schemas` | Drops the watsonx.data schemas + tables/views (calls `cleanup_watsonxdata.py`). |
| `--minio` | Deletes the demo's MinIO/S3 files (calls `cleanup_minio.py`; needs `oc`). |
| `--warehouse` | `--schemas` then `--minio` (the whole remote side). |
| `--all` | `--docker` + `--schemas` + `--minio`. |
| `--dry-run` | Show what would happen, change nothing. |
| `--keep-images` | With `--docker`, remove **no** images (only containers + volumes). |
| `--purge-base-images` | With `--docker`, **also** remove shared public base images (`postgres:16`, `metabase/metabase`, `elasticsearch`, `python:3.12-slim`). Off by default. |
| `-y`, `--yes` | Skip the confirmation prompt. |

!!! success "Scoped to this demo only — verified"
    Every deletion is bounded to this demo:

    * **Docker** — `down` is run per compose file; the leftover-volume sweep is filtered by
      Docker's own `com.docker.compose.project` label, so it can only ever match this demo's
      two projects (an unrelated stack such as `insurance_*` is never touched). Images: only
      the demo's locally built images are removed unless you pass `--purge-base-images`.
    * **Schemas** — only the exact schema names derived from your `.env`
      (`dbt_demo_*`, `spark_demo_*`, the cpdctl raw schema) are dropped.
    * **MinIO** — only the demo's own trailing-slash-scoped prefixes inside `iceberg-bucket`;
      the bucket is never emptied.

!!! danger "Still destructive — always `--dry-run` first"
    `reset_demo.sh` permanently removes the selected resources. Run `--dry-run` first to see
    the exact containers, volumes, schemas, and object counts that will go.
