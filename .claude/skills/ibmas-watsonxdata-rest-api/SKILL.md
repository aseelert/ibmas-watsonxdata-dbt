---
name: ibmas-watsonxdata-rest-api
description: "The watsonx.data v3 REST API (/lakehouse/api/v3) for THIS project's Software/CPD deployment — NEVER the SaaS pattern. Use whenever asked 'is Presto/Spark/Milvus available', 'list engines', or to check/verify any watsonx.data engine or service via API instead of oc. Triggers on phrases like: 'list spark engines', 'is presto available', 'watsonxdata-v3', 'lakehouse api v3', 'milvus_services', 'AuthInstanceId', 'check engine status via API'."
---

# watsonx.data v3 REST API — Software (CPD), not SaaS

This project runs watsonx.data **Software** (on Cloud Pak for Data), never IBM Cloud
**SaaS**. The two use *different* base URLs and slightly different auth — never use the
SaaS pattern (`https://{region}.lakehouse.cloud.ibm.com/...` with a CRN) for this project.

Everything below was **live-verified against this cluster on 2026-08-20** with real
`curl` calls (not just read from docs) — re-verify tokens/ids if this drifts, but the
mechanics are proven.

## Base URL (Software)

```
https://{WXD_CPD_HOST}/lakehouse/api/v3/{method}
```
IBM's official docs (`cloud.ibm.com/docs/apis/watsonxdata-v3#endpoint-url-software`) show
the pattern as `/lakehouse/api/v3/{instance_id}/{method}` — **empirically, the
`{instance_id}` path segment is optional** on this cluster; both
`/lakehouse/api/v3/presto_engines` and `/lakehouse/api/v3/{instance_id}/presto_engines`
return identical `200` results, as long as the instance header below is present. Prefer
the shorter form (without the path segment) to match this project's existing scripts.

## Required headers

| Header | Value | Notes |
|---|---|---|
| `Authorization` | `Bearer {WXD_SPARK_BEARER_TOKEN}` | Obtained from `WXD_CPD_AUTH_URL` (`/icp4d-api/v1/authorize`) via `python scripts/00b_get_token.py --export`, ~12h TTL. `ZenApiKey base64(username:api_key)` is IBM's documented alternative (see "Authentication software" on the API docs) — this project's `submit_spark_application.py` already falls back to it via `_derived_zen_api_key()` when a bearer token isn't set. |
| Instance context (**required, either name**) | `WXD_INSTANCE_ID` | Confirmed live: **both `AuthInstanceId` (the canonical name per the v3 spec) and `LhInstanceId` (this project's existing header name, and CPD's legacy alias) work identically** on this cluster. Omitting both → `400`/`401` depending on the endpoint (`presto_engines` 400s with a JWT-validation error; `spark_engines/.../applications` 401s). Existing scripts already send `LhInstanceId` — no change needed there, but when writing new calls either name is fine; use `AuthInstanceId` if you want to match the official spec wording. |

```bash
source .env
curl -s --cacert "$WXD_SSL_VERIFY" \
  -H "Authorization: Bearer $WXD_SPARK_BEARER_TOKEN" \
  -H "AuthInstanceId: $WXD_INSTANCE_ID" \
  "https://$WXD_CPD_HOST/lakehouse/api/v3/presto_engines"
```

## Verified endpoint map (per engine/service type)

| Type | List (GET) | Get one | Status field | Live result on this cluster |
|---|---|---|---|---|
| Presto | `/presto_engines` | `/presto_engines/{id}` | in list response | `presto653` (`ibmas-presto`) |
| Spark | `/spark_engines` | `/spark_engines/{id}` | `status` in list response | `spark588` (`ibmas-spark-java`), `status: running` — **not `spark656`**, that's a stale placeholder in `.env.example`; always trust the live API/`.env`'s `WXD_SPARK_ENGINE_ID`, not memory or examples |
| Milvus | `/milvus_services` | `/milvus_services/{id}` | in list response | present, matches `wxdengine` CR `lakehouse-milvus408` |
| Db2 | `/db2_engines` | — | — | empty (`[]`) — none configured |
| Netezza | `/netezza_engines` | — | — | empty |
| Prestissimo | `/prestissimo_engines` | — | — | empty |
| Generic/other | `/other_engines` | — | — | empty — **not** where Milvus lives, despite the name |

**OpenSearch has no `/lakehouse/api/v3` surface at all** — `GET /services` 404s
("not found in the supported APIs list"), and no `opensearch_*`/`services` method exists
in the v3 reference. OpenSearch on this cluster is managed purely at the OpenShift CRD
level (`clusters.opensearch.cloudpackopen.ibm.com`) — check it via `oc`, not this API
(see `ibmas-watsonxdata-engine-control`).

## Spark application sub-resource (used by `scripts/03b_submit_spark_application.py`)

```
GET    /spark_engines/{id}/applications              # list
POST   /spark_engines/{id}/applications               # submit
GET    /spark_engines/{engine_id}/applications/{id}   # one app's status
DELETE /spark_engines/{engine_id}/applications/{id}   # stop a running app
```
Confirmed live: `GET /spark_engines/spark588/applications` → `200`,
`{"applications":[],"limit":500,...}` with either `LhInstanceId` or `AuthInstanceId`.

## Safety tiering for this skill

- **Auto-run**: any `GET` against these endpoints — pure read, no side effects, this is
  the fastest way to answer "is X available/healthy" without `oc`.
- **Confirm first, always**: any `POST`/`PATCH`/`DELETE` against `*_engines`/`*_services`
  (creating, resizing, pausing/resuming, or deleting an engine) — same tiering as the
  CRD-level equivalents in `ibmas-watsonxdata-engine-control`.
- **Never print the resolved `Authorization`/bearer-token value** in any command output —
  interpolate it inside a script via `$WXD_SPARK_BEARER_TOKEN`, never `echo` it, and never
  pass `-v`/`--trace` on `curl` calls that carry it.
