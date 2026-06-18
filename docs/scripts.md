# Scripts Reference

All helper scripts live in `scripts/`. Run them from the **repo root** with the virtualenv active.

```bash
source .venv/bin/activate   # once per shell session
```

---

## 0 · One-time setup

### `prepare_watsonx_env.py` — import connection JSON → `.env` + SSL cert

Download the Presto connection JSON from the watsonx.data console (**Engine details → Download connection**), save it as `watsonx_data/instance_details.json`, then run:

```bash
python scripts/prepare_watsonx_env.py
```

Writes `WXD_HOST`, `WXD_PORT`, `WXD_INSTANCE_ID`, `WXD_CPD_HOST`, and the SSL cert to `certs/watsonxdata-ca.pem`. Preserves any existing `WXD_API_KEY`.

| Flag | Default | Description |
|---|---|---|
| `--connection-json PATH` | `watsonx_data/instance_details.json` | Path to the downloaded JSON |
| `--env-file PATH` | `.env` | Target env file |
| `--cert-file PATH` | `certs/watsonxdata-ca.pem` | Where to write the PEM chain |
| `--overwrite` | off | Re-import non-secret values even if already set |

---

### `get_token.py` — validate auth / refresh API key

Run this **before every session** to confirm your API key and instance ID are still valid.

```bash
python scripts/get_token.py
```

If the API key is expired it falls back to password login, regenerates a new key, and saves it to `.env` automatically.

```bash
# Force password login and regenerate API key (e.g. after a long break)
python scripts/get_token.py --refresh-key

# Also write the bearer token to .env as WXD_SPARK_BEARER_TOKEN
python scripts/get_token.py --export
```

| Flag | Description |
|---|---|
| `--refresh-key` | Skip API key, login with password, regenerate and save new key |
| `--export` | Write bearer token to `WXD_SPARK_BEARER_TOKEN` in `.env` |
| `--env-file PATH` | Override `.env` path |

??? tip "Getting an API key from the UI instead"
    1. Open `https://<WXD_CPD_HOST>` and log in as `cpadmin`.
    2. Click your avatar (top-right) → **Profile and settings** → **API key** tab.
    3. Click **Regenerate API key** → copy the key.
    4. Paste into `.env`: `WXD_API_KEY=<new-key>`.

---

## 1 · Bootstrap schemas

### `bootstrap_watsonxdata.py` — create all demo schemas in Presto

Must run **before dbt or Spark** on a fresh environment.

```bash
python scripts/bootstrap_watsonxdata.py
```

Creates four schemas in `iceberg_data`: `lakehouse_demo_raw`, `lakehouse_demo_bronze`, `lakehouse_demo_silver`, `lakehouse_demo_gold`.

---

## 2 · dbt path

### `dbt_env.sh` — dbt wrapper that loads `.env`

Use this instead of calling `dbt` directly — it sources `.env` so every dbt command picks up the correct connection vars.

```bash
# Full pipeline (recommended order)
bash scripts/dbt_env.sh seed
bash scripts/dbt_env.sh run
bash scripts/dbt_env.sh test

# Single model
bash scripts/dbt_env.sh run --select silver_orders

# Force full refresh of incremental tables
bash scripts/dbt_env.sh run --full-refresh

# Docs
bash scripts/dbt_env.sh docs generate
bash scripts/dbt_env.sh docs serve
```

---

## 3 · Spark path

### `upload_spark_assets.py` — upload app + raw CSVs to MinIO

Run **once** before submitting the Spark job. Requires `oc port-forward` to MinIO if running from a workstation.

```bash
# MinIO port-forward (separate terminal, keep open)
oc -n cpd-instance port-forward svc/ibm-lh-lakehouse-minio-svc 19000:9000

# Upload
python scripts/upload_spark_assets.py
```

Uploads `spark/app/load_medallion_demo.py` and all seed CSVs to `s3a://iceberg-bucket/spark_demo/`.

---

### `submit_spark_application.py` — submit the Spark medallion job

```bash
# Dry run (default — prints the request body, does not submit)
python scripts/submit_spark_application.py

# Real submission
WXD_SPARK_DRY_RUN=false python scripts/submit_spark_application.py
```

Prints the application ID on success. Save it — you need it for the status check.

---

### `spark_application_status.py` — check Spark job status

```bash
# Uses WXD_SPARK_APPLICATION_ID from .env
python scripts/spark_application_status.py

# Or pass the ID directly
python scripts/spark_application_status.py <application-id>
```

Polls the Spark REST API and prints state (`running`, `finished`, `failed`).

---

## 4 · Native ingestion (cpdctl)

### `ingest_with_cpdctl.py` — load CSVs via watsonx.data ingestion service

Requires `cpdctl` installed and logged in (`cpdctl config user set ...`).

```bash
python scripts/ingest_with_cpdctl.py
```

Reads CSVs from `s3a://iceberg-bucket/spark_demo/raw/` and ingests them into `lakehouse_demo_ingest` using the watsonx.data native ingestion engine.

---

## 5 · Query results

### `query_gold.py` — run reports against the gold layer

```bash
# All reports
python scripts/query_gold.py

# Single report
python scripts/query_gold.py daily_sales
python scripts/query_gold.py customer_360
```

Connects to Presto and prints formatted tables. Run `get_token.py` first if you haven't already this session.

---

## 6 · OpenMetadata

### `prepare_openmetadata_dbt_artifacts.py` — generate + stage dbt artifacts

Runs `dbt seed → run → test → docs generate` and copies the resulting JSON files to `openmetadata/dbt-artifacts/`.

```bash
python scripts/prepare_openmetadata_dbt_artifacts.py

# Skip running dbt (use existing target/*.json)
python scripts/prepare_openmetadata_dbt_artifacts.py --skip-dbt

# Skip seed step
python scripts/prepare_openmetadata_dbt_artifacts.py --skip-seed
```

---

### `upload_dbt_artifacts.py` — push staged artifacts to S3

```bash
python scripts/upload_dbt_artifacts.py
```

Uploads `manifest.json`, `catalog.json`, and `run_results.json` from `openmetadata/dbt-artifacts/` to `s3://iceberg-bucket/openmetadata/dbt-artifacts/lakehouse_demo/`. OpenMetadata reads them from there during ingestion.

---

## 7 · Demo features

### `demo_time_travel.py` — Iceberg time travel

Shows snapshot history, partition metadata, and a time-travel query against the silver layer.

```bash
python scripts/demo_time_travel.py
```

Requires the silver schema to exist (run dbt or Spark path first).

---

## 8 · Cleanup

### `cleanup_watsonxdata.py` — drop all demo schemas

```bash
python scripts/cleanup_watsonxdata.py
```

!!! danger "Destructive"
    Drops `lakehouse_demo_*` and `spark_demo_*` schemas and all their tables. Use only when tearing down the demo environment.

---

## Full demo run order

```bash
# 0 · prep
python scripts/prepare_watsonx_env.py   # once, after downloading connection JSON
python scripts/get_token.py             # every session

# 1 · bootstrap
python scripts/bootstrap_watsonxdata.py

# 2a · dbt
bash scripts/dbt_env.sh seed
bash scripts/dbt_env.sh run
bash scripts/dbt_env.sh test

# 2b · Spark (parallel option — separate schemas, does not conflict with dbt)
python scripts/upload_spark_assets.py
WXD_SPARK_DRY_RUN=false python scripts/submit_spark_application.py
python scripts/spark_application_status.py

# 3 · query
python scripts/query_gold.py

# 4 · OpenMetadata (optional)
python scripts/prepare_openmetadata_dbt_artifacts.py
python scripts/upload_dbt_artifacts.py

# 5 · time travel demo (optional)
python scripts/demo_time_travel.py

# 9 · teardown
python scripts/cleanup_watsonxdata.py
```
