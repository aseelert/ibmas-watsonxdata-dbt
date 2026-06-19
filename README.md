# watsonx.data Ingestion Workshop — dbt · Spark · cpdctl

A hands-on demo showing **two full ingest+transform pipelines (dbt, Spark)** plus **one native
ingestion loader (cpdctl)** in an IBM watsonx.data lakehouse. The same four CSV files (customers,
products, orders, order items) flow through the Bronze → Silver → Gold medallion pattern via **dbt
and Spark**. **cpdctl** lands the same CSVs as raw tables in `spark_demo_cpdctl_raw`, which you then
transform with dbt or Spark to build a medallion. No prior watsonx.data experience needed.

---

## Quick Start (5 commands)

```bash
git clone <repo-url> && cd ibmas-watsonxdata-dbt
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/prepare_watsonx_env.py     # reads watsonx_data/instance_details.json
bash scripts/dbt_env.sh run               # runs the full dbt medallion pipeline
```

!!! info "Python version"
    Python 3.11 is required. Python 3.14 currently breaks dbt through a transitive dependency.

---

## What You Will Build

**Two full medallion pipelines (dbt, Spark) plus one native ingestion loader (cpdctl)** — all writing to the same `iceberg_data` catalog, all using Iceberg table format and MinIO object storage, all queryable through the Presto SQL engine. dbt and Spark are interchangeable, self-contained ingest+transform engines; cpdctl ingests raw only and is paired with dbt or Spark to build a medallion.

| Path | Tool | Schemas written | Objects created |
|------|------|-----------------|-----------------|
| **A — dbt** (full pipeline) | dbt + Presto (SQL) | `dbt_demo_raw/bronze/silver/gold` | `gold_daily_sales` (table), `gold_category_performance` (view), `gold_customer_360` (view) |
| **B — Spark** (full pipeline) | PySpark on watsonx.data Spark engine | `spark_demo_bronze/silver/gold` | `spark_gold_daily_sales` (table), `spark_gold_category_performance` (view), `spark_gold_customer_360` (table) |
| **C — cpdctl** (ingest loader only) | IBM cpdctl CLI (native ingestion service) | `spark_demo_cpdctl_raw` | raw ingest tables (no gold): `customers`, `products`, `orders`, `order_items` |

dbt and Spark are self-contained full pipelines you can run independently and compare gold-to-gold. cpdctl is an ingest front-end — run dbt or Spark over `spark_demo_cpdctl_raw` afterward to turn its raw data into a medallion (cpdctl + dbt/Spark = one full pipeline).

!!! tip "Which path to lead with?"
    Lead with **dbt** when the story is governed SQL analytics. Use **Spark** when the story includes distributed ingestion or large-scale ETL. Use **cpdctl** when you want to show the built-in ingestion jobs that appear in the watsonx.data console under **Data manager → Ingestion (history)**.

---

## The Medallion Pattern

The medallion pattern organises data by quality — raw CSV arrives first, then each layer refines it further until it is ready for analytics.

```mermaid
flowchart LR
  CSV["CSV files\n(seeds/ or object storage)"]
  RAW["Raw\nlanding tables"]
  BRONZE["Bronze\n+ingestion metadata"]
  SILVER["Silver\ntyped & conformed"]
  GOLD["Gold\nanalytics marts"]
  SQL["Presto SQL\nBI · notebooks · demos"]

  CSV --> RAW --> BRONZE --> SILVER --> GOLD --> SQL
```

| Layer | Plain-language description | Format |
|-------|---------------------------|--------|
| Raw | Original CSV payload, unchanged, for traceability | dbt seeds / direct CSV read |
| Bronze | First managed copy in the lakehouse; adds `_ingested_at`, `_source_file`, `_ingest_batch_id` | Iceberg PARQUET table |
| Silver | Typed, cleaned, conformed entities; validated with dbt tests; orders partitioned by `month(order_date)` (partition column `order_date_month`) | Iceberg PARQUET table |
| Gold | Business-facing aggregates ready for SQL, BI, or demos | Table or view (see path) |

dbt and Spark continue from Raw through Bronze → Silver → Gold. cpdctl stops at Raw (`spark_demo_cpdctl_raw`) — it is a loader, not a full pipeline; pair it with dbt or Spark to go further:

```text
dbt path:   seeds/ CSV → dbt_demo_raw → bronze → silver → gold
Spark path: object storage CSV → spark_demo_bronze → silver → gold
cpdctl:     seeds/ CSV → spark_demo_cpdctl_raw (single-step, no medallion)
```

---

## Three Paths Compared

```mermaid
flowchart TB
  persona["Workshop participant"]
  csv["CSV demo files\n(50 customers · 20 products\n500 orders · 1134 order items)"]
  minio["MinIO object storage"]
  catalog["watsonx.data catalog: iceberg_data"]
  presto["Presto SQL engine"]
  sparkEngine["watsonx.data Spark engine"]

  dbt["Path A — dbt\nSQL models · tests · lineage"]
  sparkJob["Path B — Spark\nPySpark distributed ETL"]
  cpdctl["Path C — cpdctl\nnative ingestion CLI"]

  dbtSchemas["dbt_demo_raw/bronze/silver/gold"]
  sparkSchemas["spark_demo_bronze/silver/gold"]
  ingestSchema["spark_demo_cpdctl_raw"]

  consumers["SQL · BI · OpenMetadata · demos"]

  persona --> dbt
  persona --> sparkJob
  persona --> cpdctl

  csv --> dbt
  dbt --> presto --> catalog --> dbtSchemas

  csv --> minio
  minio --> sparkEngine
  sparkJob --> sparkEngine --> catalog --> sparkSchemas

  csv --> minio
  cpdctl --> presto --> catalog --> ingestSchema

  dbtSchemas --> consumers
  sparkSchemas --> consumers
  ingestSchema --> consumers
```

| Tool | Language | Best for | Docs |
|------|----------|----------|------|
| dbt | SQL | Governed transformations, tests, lineage, repeatable analytics models | [dbt-watsonx-presto adapter](https://docs.getdbt.com/docs/core/connect-data-platform/watsonx-presto-setup) |
| Spark | Python (PySpark) | Large-scale ingestion, complex ETL, ML feature engineering, file processing | [watsonx.data Spark docs](https://www.ibm.com/docs/en/watsonx/watsonxdata) |
| cpdctl | CLI (YAML/REST) | Built-in ingestion jobs tracked in the watsonx.data UI console | [cpdctl reference](https://github.com/IBM/cpdctl) |

---

## Documentation Site (MkDocs)

After cloning and installing dependencies, serve the full beginner-friendly docs locally.

```bash
mkdocs serve
# -> http://127.0.0.1:8000
```

`mkdocs serve` watches `docs/` and `mkdocs.yml` — edits appear instantly in the browser.
To use a different port: `mkdocs serve -a 127.0.0.1:8123`.

```bash
mkdocs build           # static output in ./site (git-ignored)
mkdocs build --strict  # fail on broken links or warnings — use in CI
```

What the docs site covers:

- **Overview** — watsonx.data, dbt, Spark, Iceberg, and the medallion pattern in plain words
- **Architecture & Lineage** — full medallion design with a column-by-column lineage diagram
- **Setup** — virtual environment, `.env`, certificates, dbt profiles
- **dbt path** — seed, run, test, query gold
- **Spark path** — upload assets, submit job, query Spark gold tables
- **cpdctl / native ingestion** — install, configure, run ingestion jobs
- **SQL demo** — copy-paste Presto queries for every layer
- **OpenMetadata** — lineage UI walkthrough
- **Glossary, File Guide, Troubleshooting**

---

## OpenMetadata (dbt Lineage UI)

OpenMetadata is an open-source data catalog that reads the JSON files dbt produces and draws the Bronze → Silver → Gold lineage graph in a browser UI. No live Presto connection is needed for the catalog UI — it only reads dbt artifact files.

```bash
# Start OpenMetadata in Docker (first run downloads ~3 GB, takes 5-10 min)
mkdir -p openmetadata
curl -fsSL \
  "https://github.com/open-metadata/OpenMetadata/releases/download/1.13.0-release/docker-compose.yml" \
  -o openmetadata/docker-compose.yml
docker compose -f openmetadata/docker-compose.yml up --detach

# Wait until the server is ready (~3-5 min)
until curl -sf http://localhost:8585/api/v1/system/version; do sleep 20; done

# Generate dbt artifacts and run ingestion (re-runnable after every dbt run)
bash scripts/dbt_env.sh docs generate --no-compile
cp target/manifest.json target/catalog.json target/run_results.json openmetadata/dbt-artifacts/
source .venv/bin/activate
bash openmetadata/ingestion/run-ingestion.sh
```

Open **http://localhost:8585** and log in with `admin@open-metadata.org` / `admin`.

Navigate to **Explore → Databases → watsonxdata-presto → iceberg_data → dbt_demo_gold → gold_daily_sales → Lineage** to see the full medallion graph.

```bash
# Stop OpenMetadata when done
docker compose -f openmetadata/docker-compose.yml down
```

!!! note "Version"
    This repo is tested against OpenMetadata 1.13.0. The full walkthrough is in `docs/openmetadata.md`.

---

## Prerequisites

Before cloning, confirm you have:

- [ ] **Python 3.11** — `python3.11 --version`
- [ ] **Git** — `git --version`
- [ ] **OpenShift CLI (`oc`)** — Spark + cpdctl paths (reads the MinIO secret and port-forwards to object storage). Install the **latest** in [Setup → Step 8](docs/setup.md#step-8-install-command-line-tools-oc-cpdctl).
- [ ] **IBM `cpdctl`** — cpdctl path only. Install the **latest** in [Setup → Step 8](docs/setup.md#step-8-install-command-line-tools-oc-cpdctl).
- [ ] **Docker Desktop** — running (needed for OpenMetadata only)
- [ ] **watsonx.data credentials** — API key, Presto host, instance ID, and the connection JSON exported from the watsonx.data console

See the [full setup guide](docs/setup.md) for which tools each path needs and the complete
`.env` / dbt-profile reference.

!!! info "Connection JSON"
    Export the Presto connection JSON from the watsonx.data console and save it as `watsonx_data/instance_details.json`.
    Then run `python scripts/prepare_watsonx_env.py` — it populates `.env` and writes `certs/watsonxdata-ca.pem` automatically.

---

## Security Note

Do not commit watsonx.data API keys. Put credentials in your shell environment or a local `.env` file (which is git-ignored). If an API key was pasted into chat or committed anywhere, rotate it before customer demos.
