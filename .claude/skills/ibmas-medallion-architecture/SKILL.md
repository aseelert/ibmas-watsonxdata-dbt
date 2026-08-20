---
name: ibmas-medallion-architecture
description: "Use when explaining or reasoning about the Bronze/Silver/Gold medallion pattern across this project's 4 interchangeable paths (dbt, Spark, DataStage, Confluent), or when you need to know what a WXD_*/PG_*/CONFLUENT_* env var means and what derives it. Triggers on phrases like: 'medallion layer', 'bronze silver gold', 'which schema', 'what does WXD_ mean', 'env var glossary', 'compare the four paths', 'schema naming'."
---

# Medallion Architecture Reference (4 paths, 1 pattern)

## Overview

One Bronze→Silver→Gold pipeline, built 4 different ways, all writing Iceberg/PARQUET
tables into the **same** `iceberg_data` catalog on MinIO, all queryable via Presto:

| Path | Engine | Schema prefix | Entry point |
|---|---|---|---|
| **dbt** (SQL) | Presto | `dbt_demo_{raw,bronze,silver,gold}` | `scripts/dbt_env.sh run` |
| **Spark** | watsonx.data Spark engine | `spark_demo_{bronze,silver,gold}` | `scripts/submit_spark_application.py` |
| **cpdctl** (native ingest) | — | `spark_demo_cpdctl_raw` (raw only) | `scripts/ingest_with_cpdctl.py` |
| **DataStage** | Presto (SQL-pushdown via `lakehouse` connector) | `datastage_demo_{bronze,silver,gold}` | `scripts/datastage/create_medallion_flows.py` |
| **Confluent** (streaming, newer) | Flink (silver) + Spark or DataStage (gold) | `confluent_demo_{silver,gold}` | `confluent/start.sh` |

All 5 read the same 4 seed CSVs (50 customers, 20 products, 500 orders, 1134 order_items)
and — when the gold engine is dbt-sourced logic — must produce **identical gold numbers**
(verified precedent: gold sums = $87,509.85 across dbt/DataStage/Spark).

Schema names never overlap across paths on purpose — this lets a demo show all paths
side-by-side in the same catalog without collision.

---

## Env var glossary (what derives what)

Source of truth: `.env` (git-ignored), bootstrapped by `python3 scripts/prepare_watsonx_env.py`
from a Presto connection JSON exported from the watsonx.data UI + OpenShift secrets.

### Presto / dbt core
| Var | Meaning | Derivation |
|---|---|---|
| `WXD_HOST` / `WXD_PORT` | Presto engine's service hostname:443 | AUTO from connection JSON `engine_host`/`engine_port` — **drifts**: engine pods get recreated with a new numeric suffix (e.g. `presto651`→`presto653`); always re-run `prepare_watsonx_env.py` rather than trust a stale value |
| `WXD_INSTANCE_ID` | watsonx.data instance id | AUTO from JSON `instance_id` |
| `WXD_PRESTO_ENGINE_ID` | Current Presto engine short id (e.g. `presto653`) | AUTO from JSON `engine_id` — confirm against live `oc get wxdengine -A` before trusting |
| `WXD_USER` | Presto auth username, always `ibmlhapikey_<software-hub-user>` | MANUAL |
| `WXD_API_KEY` | Software Hub API key = the Presto password | **SECRET** — never written by any script, generate in Software Hub UI → Profile → API key |
| `WXD_CATALOG` | Iceberg catalog name | AUTO default `iceberg_data` |
| `WXD_SCHEMA` | Base schema prefix for dbt | AUTO default `dbt_demo` — override to rename the whole demo's dbt footprint in one place |
| `WXD_{RAW,BRONZE,SILVER,GOLD}_SCHEMA` | Per-layer override | Optional — falls back to `{WXD_SCHEMA}_<layer>` |
| `WXD_GOLD_MATERIALIZED` | `table` or `view` for gold models | AUTO default `view` |
| `WXD_SSL_VERIFY` | CA bundle path for TLS to Presto | AUTO — written to `certs/watsonxdata-ca.pem` |

### CPD / OpenShift
| Var | Meaning |
|---|---|
| `WXD_CPD_HOST` | Software Hub route hostname |
| `WXD_CPD_AUTH_URL` | `/icp4d-api/v1/authorize` — bearer-token auth endpoint |
| `WXD_CPD_PASSWORD` | cpadmin password, read from an OCP secret via `oc` |
| `WXD_OPENSHIFT_API` / `WXD_OPENSHIFT_CONSOLE` | `api.<domain>:6443` / console route |
| `WXD_OPENSHIFT_NAMESPACE` | `cpd-instance` — where every `wxd*`/`wkc*`/`datastage*` CR lives |
| `WXD_CPD_PROJECT` | CPD project owning demo connections/assets, default `ibmas-ingest-demo` |
| `WXD_OC_USER` / `WXD_OC_PASSWORD` / `WXD_OC_TOKEN` | oc login creds — password is the only value `prepare_watsonx_env.py` cannot auto-discover |

### Spark
| Var | Meaning |
|---|---|
| `WXD_SPARK_ENGINE_ID` | The **watsonx.data-registered** Spark engine id used in the `lakehouse/api/v3/spark_engines/<id>` REST path — a logical registration, distinct from the underlying `AnalyticsEngine` k8s CR. Live-verified value on this cluster: `spark588`; drifts, always confirm via `.env` or `GET /lakehouse/api/v3/spark_engines` rather than trusting a remembered id (see `ibmas-spark-analyticsengine-ops`, `ibmas-watsonxdata-rest-api`) |
| `WXD_SPARK_APPLICATIONS_ENDPOINT` | Full REST URL scripts POST to for job submission |
| `WXD_SPARK_BEARER_TOKEN` | **SECRET**, ~12h TTL — refresh via `python scripts/get_token.py --export` before every submit session |
| `WXD_SPARK_DRY_RUN` | AUTO default `true` — submit script prints the redacted payload and exits 0 until flipped |
| `WXD_SPARK_{CATALOG,SCHEMA,BRONZE_SCHEMA,...}` | Spark's own schema namespace, kept separate from dbt's |

### PostgreSQL reporting
| Var | Meaning |
|---|---|
| `PG_HOST` | `<cluster>-primary.<namespace>.svc.cluster.local` — **Crunchy naming is `-primary`, not `-rw`** (that's CloudNativePG/EDB, a different operator) |
| `PG_SSL_MODE` | Must stay `require` — Crunchy's `pg_hba.conf` only permits `hostssl`, `disable` is rejected outright |
| `PG_GOLD_SCHEMA` | Source gold schema mirrored into reporting tables |

### Bastion / network (documented, not auto-derived)
| Var | Meaning |
|---|---|
| `CLUSTER_BASTION_HOST` | `9.82.206.23` — SSH jump box for reaching the cluster from outside; day-to-day `oc`/`kubectl`/`cpdctl` do **not** need it (see `ibmas-cpd-cluster-ops-runbook`) |
| `CLUSTER_APP_DOMAIN` | Wildcard domain every Route hangs off, e.g. `apps.watson.ibmas-zocp-techcluster.org` |

See `ibmas-dbt-medallion-modeling`, `ibmas-spark-analyticsengine-ops`, `ibmas-watsonxdata-engine-control` for the deep per-path/per-engine detail this glossary points at.
