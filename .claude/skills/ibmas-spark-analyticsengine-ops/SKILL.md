---
name: ibmas-spark-analyticsengine-ops
description: "Use for the Spark medallion path in this project — both the OpenShift-level AnalyticsEngine CR and the watsonx.data-registered Spark engine that jobs actually submit to. Triggers on phrases like: 'submit spark job', 'spark engine status', 'analytics engine', 'spark bearer token', 'spark medallion', 'spark656', 'is spark healthy'."
---

# Spark / AnalyticsEngine Ops

## Three layers — don't conflate them

1. **k8s compute**: CRD `analyticsengines.ae.cpd.ibm.com`, this cluster's instance
   `analyticsengine-sample` (version 5.3.6, ns `cpd-instance`) — the actual OpenShift
   Analytics Engine operand. Check with:
   ```bash
   oc get analyticsengines -A -o wide
   ```
   **This is not a `wxdengine`** — Presto and Milvus are `wxdengine`s; Spark is not.
2. **watsonx.data registration**: a logical "Spark engine" (env var `WXD_SPARK_ENGINE_ID`)
   registered inside watsonx.data's own engine catalog, addressed via
   `lakehouse/api/v3/spark_engines/<id>/applications` — this is what
   `scripts/03b_submit_spark_application.py` actually POSTs to. It's a binding on top of the
   AnalyticsEngine compute, not a separate k8s object — you won't find it via `oc get`.
   **Live-verified on this cluster (2026-08-20): the current id is `spark588`**
   (`display_name: ibmas-spark-java`, `status: running`, `type: spark`, `origin: native`)
   — **`spark656` is a dead id, but it is not just a doc example**: it's still hardcoded as
   a fallback default in `scripts/03c_spark_application_status.py`,
   `scripts/03b_submit_spark_application.py`, and `scripts/04_ingest_with_cpdctl.py` (used only if
   the env var is unset), plus `.env.example`/`.env.backup`. Harmless while `.env` has the
   correct `WXD_SPARK_ENGINE_ID`, but if `.env` ever fails to load, these scripts silently
   target the dead `spark656` id instead of erroring. Always confirm via
   `GET /lakehouse/api/v3/spark_engines` (see `ibmas-watsonxdata-rest-api`) or `.env`'s
   actual `WXD_SPARK_ENGINE_ID`, never hardcode an id from memory.
3. **The local job**: `spark/load_medallion_demo.py` — the actual PySpark medallion
   transformation code, submitted as an `application_details.application` URI in the
   payload above.

If someone asks "is Spark healthy," check layer 1 (`oc get analyticsengines`); if they ask
"why did my job fail," check layer 2/3 (application status + bearer token).

## Submitting a job

```bash
python scripts/00b_get_token.py --export          # refresh WXD_SPARK_BEARER_TOKEN, ~12h TTL
python scripts/03b_submit_spark_application.py    # DRY RUN by default (WXD_SPARK_DRY_RUN=true)
```
Set `WXD_SPARK_DRY_RUN=false` in `.env` to actually submit — the dry-run default is a
deliberate safety rail, don't flip it without the user asking to actually run the job.

The submit script propagates env (input base, catalog, schema, batch id) to driver AND
executors via every prefix the engine might honor (`spark.executorEnv.*`,
`spark.yarn.appMasterEnv.*`, `spark.driverEnv.*`, `spark.kubernetes.driverEnv.*`), and
injects the watsonx.data data-access key as `spark.hadoop.wxd.apiKey`. It also
best-effort pre-creates the bronze/silver/gold namespaces **through Presto** so they land
at the catalog warehouse root with no Hive `.db` suffix — the Iceberg catalog ignores
`CREATE NAMESPACE ... LOCATION`, Presto is the only lever for on-disk layout. A failure
here is non-fatal to the submit.

## Monitoring and follow-up

```bash
python scripts/03c_spark_application_status.py <app_id>     # poll to terminal state
python scripts/create_gold_views.py --path spark         # AFTER app finishes: Spark writes
                                                           # only daily_sales as a table; the
                                                           # category/customer_360 gold marts
                                                           # need Presto views on top, since a
                                                           # Spark-created Hive view isn't
                                                           # readable from Presto
```

## Auth (any one of these env vars, checked in order by the submit script)

`WXD_SPARK_BEARER_TOKEN` (preferred) → `WXD_ZEN_API_KEY` → `WXD_CPD_USERNAME`/`WXD_USER` +
`WXD_CPD_API_KEY`/`WXD_API_KEY` → `WXD_CPD_PASSWORD` (via `WXD_CPD_AUTH_URL`). HTTP 401 from
the submit script almost always means the bearer token expired — re-run `scripts/00b_get_token.py`.

The script sends the instance context as `LhInstanceId: $WXD_INSTANCE_ID` — **confirmed
live-equivalent to `AuthInstanceId`** on this cluster, no change needed. See
`ibmas-watsonxdata-rest-api` for the full verified REST reference (base URL, both header
names, the full engine/service endpoint map) if writing any new REST call beyond what the
existing scripts already do.

## Safety tiering for this skill

- **Auto-run**: status checks (`oc get analyticsengines`, `scripts/03c_spark_application_status.py`),
  token refresh, dry-run submits (the default).
- **Confirm first**: flipping `WXD_SPARK_DRY_RUN=false` to actually submit a live job, any
  `oc patch`/scale on the `analyticsengine-sample` CR.
