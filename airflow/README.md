# Airflow orchestration for the watsonx.data medallion demo

Local **Apache Airflow 3** that schedules the two medallion pipelines you
normally run by hand — one through **dbt + Presto**, one through **watsonx.data
Spark** — proving Airflow can drive either engine on an hourly cadence.

Everything is **local** (Airflow, dbt, Postgres metadata DB). Only **Presto and
Spark are remote** on the on-prem watsonx.data cluster.

```
┌─────────────────────────── your laptop (Docker) ───────────────────────────┐
│  Airflow 3  ──────────────┐                                                 │
│   api-server (UI :8082)   │   dbt_medallion_hourly  ── Presto/HTTPS ──┐     │
│   scheduler               ├──►                                        │     │
│   dag-processor           │   spark_medallion_hourly ── REST/HTTPS ──┐│     │
│   Postgres (metadata)     │                                          ││     │
└───────────────────────────┘                                          ││     │
                                                                        ▼▼     │
                                       ┌──────── watsonx.data (on-prem) ──────┐
                                       │  Presto engine   Spark engine        │
                                       │  Iceberg catalog (iceberg_data)      │
                                       └──────────────────────────────────────┘
```

## The two DAGs

### `dbt_medallion_hourly` — one Airflow task per dbt model
Mirrors the manual flow `bootstrap → seed → run → test → query`, but with the
Airflow graph wired to the **exact dbt `ref()` lineage**, one task per table:

```
bootstrap_schemas
 ├─raw────  seed_raw_customers/products/orders/order_items
 ├─bronze─  bronze_*            (each ← its raw seed)
 ├─silver─  silver_*            (each ← its bronze)
 │          silver_sales_enriched (← all 4 silver)   time_spine_daily
 ├─gold───  gold_daily_sales → gold_category_performance
 │          gold_customer_360  (← silver_customers, silver_sales_enriched)
 ├─        dbt_test
 └─        query_gold          (scripts/query_gold.py)
```
Targets `lakehouse_demo_{raw,bronze,silver,gold}`.

### `spark_medallion_hourly` — one Spark submission, per-layer verification
```
prepare: upload_assets (optional)   auth: get_token
build:   submit_spark_app → wait_for_spark
verify:  validate_bronze → validate_silver → validate_gold   (Presto counts)
query:   query_gold_spark
```
Targets `spark_demo_{bronze,silver,gold}`.

**Why the Spark build is a single task:** `spark/load_medallion_demo.py` writes
the whole medallion (bronze→silver→gold) in one distributed job — that is how
Spark works, and we must not edit that app. So the submit stays one task and we
give **each layer its own verification task** instead, keeping the same
raw→bronze→silver→gold shape as the dbt DAG, honestly.

## Design principles

| Principle | How it's enforced |
|-----------|-------------------|
| **Single source of truth** | Every `WXD_*` value comes from `.env` via compose `env_file`. The compose `environment:` block holds only Airflow/dbt infra paths — no business config is duplicated. |
| **No copies** | The repo is bind-mounted **read-only** at `/opt/airflow/project`. The TLS cert `certs/watsonxdata-ca.pem` is read straight from there. |
| **Reuse, don't reimplement** | DAGs call the existing scripts (`bootstrap_watsonxdata.py`, `query_gold.py`, `upload_spark_assets.py`). Shared auth/TLS/Presto logic lives once in `airflow/dags/common/wxd.py`, mirroring the scripts. |
| **Read-only project** | dbt's writable dirs are redirected via `DBT_TARGET_PATH` / `DBT_LOG_PATH` into the logs volume, so the mounted repo stays pristine. |
| **Resilient auth** | `get_token` mints a fresh CPD bearer token from the long-lived `WXD_API_KEY` on every run — no stale tokens in scheduled runs. |

## Is this "the right way"?

Yes, with one deliberate teaching choice:

- **dbt-in-Airflow best practice** is usually [astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos),
  which auto-renders the dbt manifest into one task per model. We instead use
  explicit per-model `BashOperator`s so the medallion is obvious and dependency-
  free for a demo. Both produce the same per-table graph; Cosmos is the
  production upgrade path.
- **Spark-in-Airflow best practice** = submit + poll with a **reschedule-mode
  sensor** (frees the worker between pokes). That's exactly what we do.
- **Secrets**: the API key is injected via env, and the Spark `apiKey` is
  redacted in logs. For production, move it to an Airflow Connection/Variable
  backed by a secrets backend.

## Run it

```bash
# from the repo root
cp .env.example .env                                # fill in your values
cp profiles/profiles.example.yml profiles/profiles.yml

docker compose -f docker-compose-airflow.yml build
docker compose -f docker-compose-airflow.yml up airflow-init      # one-shot
docker compose -f docker-compose-airflow.yml up -d

open http://localhost:8082          # login: admin / admin
```

Unpause a DAG in the UI (or trigger it). Port **8082** is used so it never
clashes with OpenMetadata's Airflow on 8080.

### Triggering from the CLI (Airflow 3 REST API)
```bash
TOKEN=$(curl -s http://localhost:8082/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

curl -s -X PATCH http://localhost:8082/api/v2/dags/dbt_medallion_hourly \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"is_paused": false}'

curl -s -X POST http://localhost:8082/api/v2/dags/dbt_medallion_hourly/dagRuns \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"logical_date":"2026-06-19T12:00:00Z"}'
```

## Files
```
docker-compose-airflow.yml     # 4 services: api-server, scheduler, dag-processor, postgres
airflow/Dockerfile             # apache/airflow:3.x + dbt-watsonx-presto + boto3/prestodb
airflow/requirements.txt
airflow/dags/
  common/wxd.py                # shared auth / TLS / Presto helpers (single source)
  dag_dbt_medallion.py
  dag_spark_medallion.py
```

## Notes / gotchas (Airflow 3)
- `webserver` is now **`api-server`**; health is at `/api/v2/monitor/health`.
- **DAG parsing** is a separate `dag-processor` service (not in the scheduler).
- Auth uses the native **SimpleAuthManager** (no Flask-AppBuilder).
- `upload_assets` needs the MinIO object store reachable from the container; it
  is **off by default** — run `python scripts/upload_spark_assets.py` on the
  host once instead (it handles the `oc port-forward`).
