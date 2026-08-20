---
name: ibmas-watsonxdata-engine-control
description: "Use for deep watsonx.data-specific operations — NOT generic Presto SQL. Covers the wxd/wxdengine/wxdaddon CRDs, engine sizing presets (Starter/Small/Medium/Large, mincpureq), Presto/Milvus/OpenSearch lifecycle, and the non-Premium capability boundary. Triggers on phrases like: 'resize presto', 'is watsonx.data healthy', 'wxdengine', 'wxd CR', 'presto engine sizing', 'milvus status', 'opensearch status', 'watsonx.data premium', 'engine OOM'."
---

# watsonx.data Engine Control (wxd / wxdengine / wxdaddon)

This is about the **watsonx.data control plane on OpenShift** — the CRDs IBM's operators
reconcile — not native Presto/Trino administration. Everything here is verified against
this project's live cluster (`cpd-instance` namespace); re-verify before trusting stale
values, engine ids and sizes drift as pods get recreated.

## CRD family (`watsonxdata.ibm.com`)

| CRD | This cluster's instance | Purpose |
|---|---|---|
| `wxds` | `lakehouse` — version **2.3.4**, size `small`, `shutdown="false"` | The top-level watsonx.data instance |
| `wxdengines` | `lakehouse-presto653` (type `presto`, size `starter`, RUNNING); `lakehouse-milvus408` (type `milvus`, size `custom`, RUNNING) | Query/vector engines. **Spark is NOT here** — see `ibmas-spark-analyticsengine-ops` |
| `wxdaddons` | `wxdaddon` — size `small_mincpureq`, Completed | The **Ansible operator that is the controlling authority** for wxd start/stop — see hazard below |
| `wxdaddonpremiums` | *(no instance on this cluster)* | Its absence is the proof this is watsonx.data **non-Premium** — no gen-AI unstructured pipeline / Docling |

OpenSearch runs under a *different* operator family entirely — `clusters.opensearch.cloudpackopen.ibm.com`
(this cluster's instance: `elasticsearch-master`, v3.5.0, 3/3 ready) — don't look for it under `wxdengine`.
It also has **no watsonx.data v3 REST surface** — check it via `oc` only.

## Faster health check: the watsonx.data v3 REST API (no `oc` needed)

For "is Presto/Milvus available" questions, `oc get wxdengine` works, but the
watsonx.data REST API answers the same question in one HTTP call and also returns
richer per-engine metadata (associated catalogs, actions the caller can take, etc.).
See `ibmas-watsonxdata-rest-api` for the full verified reference; short version:
```bash
source .env
curl -s --cacert "$WXD_SSL_VERIFY" -H "Authorization: Bearer $WXD_SPARK_BEARER_TOKEN" \
  -H "AuthInstanceId: $WXD_INSTANCE_ID" "https://$WXD_CPD_HOST/lakehouse/api/v3/presto_engines"
curl -s --cacert "$WXD_SSL_VERIFY" -H "Authorization: Bearer $WXD_SPARK_BEARER_TOKEN" \
  -H "AuthInstanceId: $WXD_INSTANCE_ID" "https://$WXD_CPD_HOST/lakehouse/api/v3/milvus_services"
```
This is Software (CPD), never the SaaS `lakehouse.cloud.ibm.com` pattern — the skill above
is explicit about that distinction.

## Sizing model (node TYPE + node COUNT presets)

| Preset | Topology |
|---|---|
| Starter | 1 coordinator + 1 worker |
| Small | 1 coordinator + 3 cache-optimized workers |
| Medium | 1 coordinator + 6 workers |
| Large | 1 coordinator + 12 workers |
| `mincpureq` suffix (seen on `wxdaddon`) | Minimum-CPU-request variant of a size, for constrained clusters |
| `custom` (seen on the Milvus engine) | Manually-sized, not one of the fixed presets |

**Two single-node Presto pods is not a valid topology** — if you see that, something patched
the wrong resource. Sizing is normally preset-driven via the CR (no free-form CPU/mem field on
a `starter`/`small` engine) — a customer console-driven resize is the supported lever;
`oc patch` directly on the underlying StatefulSet is a stopgap, not the fix (see below).

## Known-good fix: Presto OOMKilled under heavy load

Symptom: coordinator/worker pod exit code 137 during heavy MDE profiling/DQ jobs on a
`starter`/single-node engine (1 CPU / 4G default). Fix that held without the operator
reverting it:
```
oc patch sts ibm-lh-lakehouse-presto<NNN>-single-blue \
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"<container>","resources":{"requests":{"memory":"10Gi","cpu":"2"},"limits":{"memory":"10Gi","cpu":"2"}}}]}}}}'
```
This is a **HIGH-RISK action** — confirm with the user before running; it survived one
cycle because single-node engines have no CR-level size field to fight with, but a
console-driven engine restart *could* still revert it.

## The `shutdown` flag is a STRING, not a boolean

```
oc -n cpd-instance get wxd lakehouse -o jsonpath='{.spec.shutdown}'
oc -n cpd-instance get wxdaddon wxdaddon -o jsonpath='{.spec.shutdown}'
```
Both must read the literal string `"false"`. A quiesced cluster (after shutdown) scales
Deployments to `0/0`, and `0/0` satisfies "ready == desired" — so replica-count-based
health checks report **green on a fully shut-down cluster**. Only these two CR flags are
authoritative for "is wxd actually supposed to be up."

## Safety tiering for this skill

- **Auto-run**: `oc get wxd*/wxdengine/wxdaddon -A`, `oc describe`, checking pod status/logs,
  reading the `spec.shutdown` flags above.
- **Confirm first, always**: any `oc patch` touching `spec` (sizing, `shutdown`), any engine
  restart via the addon, anything that could scale a Deployment/StatefulSet to 0.

See `ibmas-cpd-cluster-ops-runbook` for the wxdAddon shutdown/restart procedure itself and
the operator-restart recovery hazard, and `ibmas-watsonxdata-rest-api` for the full REST
API reference (auth headers, endpoint map, safety tiering for API-level mutations).
