# openlineage-marquez — Marquez OpenLineage Collector

This folder contains everything needed to run [Marquez](https://marquezproject.ai) as the OpenLineage collector for this demo.  
There are **two completely independent deployments** — one per runtime environment:

| | dbt lineage | Spark lineage |
|---|---|---|
| **Runs on** | Your Mac (laptop) | OpenShift `cpd-instance` pods |
| **Deployment** | Docker Compose (this folder) | Helm + EDB PostgreSQL (`ocp/`) |
| **URL** | `http://localhost:5010` | `http://marquez.cpd-instance.svc.cluster.local:5000` |
| **Env var** | `OPENLINEAGE_URL` | `OPENLINEAGE_SPARK_URL` |
| **Namespace in UI** | `dbt_demo` | `watsonxdata-spark` |
| **Web UI** | http://localhost:3001 | https://marquez-web.apps.watson.ibmas-zocp-techcluster.org |

These two setups are **fully independent** — both can be active simultaneously without conflict.  
The Mac never talks to the in-cluster service. Spark pods never talk to localhost.

---

## ❶ Local — Mac / Docker Compose (dbt lineage)

### Start

```bash
# from the repo root
docker compose up -d marquez-api marquez-web
# or from this folder directly
cd openlineage-marquez && docker compose up -d marquez-api marquez-web
```

### `.env` settings

```dotenv
OPENLINEAGE_URL=http://localhost:5010    # ← Mac only
OPENLINEAGE_NAMESPACE=dbt_demo
```

### Trigger lineage events

```bash
bash scripts/02_dbt_env.sh run --select bronze
# or full run:
bash scripts/02_dbt_env.sh run
```

`02_dbt_env.sh` automatically calls `scripts/emit_openlineage_events.py` after  
every successful `dbt run`, which POSTs OpenLineage events to `OPENLINEAGE_URL`.

To emit events manually (e.g. after changing models without re-running):

```bash
.venv/bin/python3 scripts/emit_openlineage_events.py --project-dir ./
```

### Verify namespaces

```bash
curl -s http://localhost:5010/api/v1/namespaces | python3 -m json.tool
```

Expected: `dbt_demo` appears in the list.

### Browse lineage

Open http://localhost:3001 → **Namespaces** → `dbt_demo` → **Jobs** → any model → **Lineage** tab.

### Ports

| Service | Port | Note |
|---|---|---|
| Marquez API | 5010 | `OPENLINEAGE_URL` base — not 5000 (macOS AirPlay) |
| Marquez Admin | 5012 | healthcheck / metrics — internal |
| Marquez Web | 3001 | not 3000 (Metabase already uses 3000) |

### Disable

Comment out `OPENLINEAGE_URL` in `.env`.  
`02_dbt_env.sh` falls back to plain `dbt` — zero overhead, zero events.

---

## ❷ OpenShift — Helm + EDB PostgreSQL (Spark lineage)

All OCP manifests and the install script live in `ocp/`.

### Files

| File | Purpose |
|---|---|
| `ocp/install-marquez-ocp.sh` | Full idempotent install: EDB PG cluster + Helm Marquez + Routes |
| `ocp/01-marquez-postgres-edb.yaml` | EDB PostgreSQL 16 cluster (`marquez-postgres`) |
| `ocp/02-marquez-values-ocp.yaml` | Helm values — points at `marquez-db-secret`, disables built-in PG |
| `ocp/03-routes.yaml` | OpenShift Routes: `marquez-api` + `marquez-web` (edge TLS) |
| `ocp/04-spark-openlineage-integration.yaml` | Reference doc — Option A (per-job) + Option B (CR patch) |

### Install (idempotent — safe to re-run)

```bash
# log in first
oc login https://api.watson.ibmas-zocp-techcluster.org:6443

bash openlineage-marquez/ocp/install-marquez-ocp.sh
```

### Status check

```bash
oc get pods -n cpd-instance -l app.kubernetes.io/name=marquez
# Expected: marquez-* and marquez-web-* Running

curl -sk https://marquez-api.apps.watson.ibmas-zocp-techcluster.org/api/v1/namespaces
```

### `.env` settings (Spark only — these are NOT used by dbt)

```dotenv
OPENLINEAGE_SPARK_URL=http://marquez.cpd-instance.svc.cluster.local:5000
OPENLINEAGE_SPARK_NAMESPACE=watsonxdata-spark
```

`scripts/03b_submit_spark_application.py` injects these as  
`spark.openlineage.transport.url` / `spark.openlineage.namespace`  
into every Spark job submission automatically.

### Web UI (browser)

https://marquez-web.apps.watson.ibmas-zocp-techcluster.org  
→ **Namespaces** → `watsonxdata-spark` → **Jobs** → any Spark job → **Lineage** tab

---

## Architecture overview

```
Mac (your laptop)                       OpenShift cpd-instance
─────────────────────────────────       ──────────────────────────────────────
dbt run                                 Spark executor pods
  └─ 02_dbt_env.sh                        └─ 03b_submit_spark_application.py
       └─ emit_openlineage_events.py            └─ _inject_openlineage_conf()
            │                                         │
            │ POST /api/v1/lineage                    │ POST /api/v1/lineage
            ▼                                         ▼
    localhost:5010                     marquez.cpd-instance.svc.cluster.local:5000
    (Docker Compose)                   (K8s ClusterIP service — no TLS)
            │                                         │
            ▼                                         ▼
    marquez-db (Postgres)              marquez-postgres (EDB PG 16 cluster)
            │                                         │
            ▼                                         ▼
    localhost:3001                     marquez-web.apps.watson...org
    (web UI — local)                   (OCP Route — browser only)

OPENLINEAGE_URL=http://localhost:5010  OPENLINEAGE_SPARK_URL=http://marquez...5000
OPENLINEAGE_NAMESPACE=dbt_demo         OPENLINEAGE_SPARK_NAMESPACE=watsonxdata-spark
```

These two paths **share no infrastructure** — they are fully independent.

---

## Troubleshooting

### dbt events not appearing in local Marquez

1. Is Docker running? `docker compose ps` (from `openlineage-marquez/`)
2. Is `OPENLINEAGE_URL=http://localhost:5010` set in `.env`?
3. Run manually: `.venv/bin/python3 scripts/emit_openlineage_events.py --project-dir ./`
4. Check API: `curl -s http://localhost:5010/api/v1/namespaces`

### Spark jobs not appearing in OCP Marquez

1. Is `OPENLINEAGE_SPARK_URL` set in `.env`?
2. Did `_inject_openlineage_conf()` fire? Look for `[OpenLineage]` lines in the Spark submission log.
3. The in-cluster URL is NOT reachable from your Mac — check from inside a pod:
   ```bash
   oc exec -n cpd-instance deploy/marquez -- curl -s http://marquez:5000/api/v1/namespaces
   ```
4. Check Marquez logs: `oc logs -n cpd-instance deploy/marquez -f`
