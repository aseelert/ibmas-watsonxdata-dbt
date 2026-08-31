# Marquez on OpenShift — Deployment & Operations Guide

**Status:** ✅ Live in `cpd-instance` (deployed 2026-08-28)  
**Marquez version:** 0.51.1  
**PostgreSQL:** EDB PostgreSQL 16.13 (single instance)  
**Cluster:** OpenShift 4.19 / CPD 5.3 — `api.watson.ibmas-zocp-techcluster.org`

---

## What is this?

[Marquez](https://github.com/MarquezProject/marquez) is an open-source metadata server
that implements the [OpenLineage](https://openlineage.io/) standard. It collects lineage
events from Spark, dbt, Airflow and other tools, and lets you visualise data pipeline
dependencies, job runs, and dataset provenance.

This deployment wires Marquez into the `cpd-instance` namespace so that:
- **watsonx.data Spark jobs** emit lineage events automatically via the
  `openlineage-spark` listener.
- **dbt runs** (this project) emit events via `scripts/emit_openlineage_events.py`.
- Any other job in the namespace can POST to the in-cluster endpoint.

---

## URLs

| Endpoint | URL |
|---|---|
| Web UI | https://marquez-web.apps.watson.ibmas-zocp-techcluster.org |
| API (external) | https://marquez-api.apps.watson.ibmas-zocp-techcluster.org |
| API (in-cluster) | `http://marquez.cpd-instance.svc.cluster.local:5000` |
| OpenLineage receiver | `…/api/v1/lineage` |
| Namespaces list | `…/api/v1/namespaces` |

The external URLs are OpenShift Routes with **edge TLS** (HTTPS → HTTP inside the pod).
In-cluster callers (e.g. Spark executors) use the ClusterIP service — no TLS, no auth.

---

## Architecture

```
Workstation / dbt
  └─ scripts/emit_openlineage_events.py
       └─→ POST https://marquez-api.apps.…/api/v1/lineage
             │
             ▼ (edge TLS route → ClusterIP)
        ┌─────────────────────────────────────────────┐
        │  cpd-instance namespace                     │
        │                                             │
        │  marquez (API pod, port 5000)               │
        │    ├── reads/writes ─→ marquez-postgres-rw  │
        │    │                    (EDB PG 16, 10Gi)   │
        │    └── serves web ─→ marquez-web (port 3000)│
        │                                             │
        │  spark job (AE pod)                         │
        │    └─→ POST http://marquez:5000/api/v1/lineage│
        └─────────────────────────────────────────────┘
```

---

## Files in this repo

| File | Purpose |
|---|---|
| `openlineage-marquez/ocp/01-marquez-postgres-edb.yaml` | EDB `Cluster` CR + NetworkPolicy |
| `openlineage-marquez/ocp/02-marquez-values-ocp.yaml` | Helm values (images, DB ref, resources) |
| `openlineage-marquez/ocp/03-routes.yaml` | OpenShift Routes (edge TLS) |
| `openlineage-marquez/ocp/04-spark-openlineage-integration.yaml` | Spark conf reference (per-job and cluster-wide) |
| `openlineage-marquez/ocp/install-marquez-ocp.sh` | One-shot install/upgrade/uninstall script |
| `scripts/emit_openlineage_events.py` | Emit dbt lineage events to Marquez |

---

## Initial installation

```bash
# Prerequisites: oc (logged in), helm >= 3.10, git
oc whoami   # confirm: kube:admin or cluster-admin

# Step 1 — EDB PostgreSQL (apply once; idempotent after)
oc apply -f openlineage-marquez/ocp/01-marquez-postgres-edb.yaml

# Wait for EDB to initialise (~30–90 s)
oc get cluster.postgresql.k8s.enterprisedb.io marquez-postgres -n cpd-instance -w

# Step 2 — Full install (Helm + Routes)
bash openlineage-marquez/ocp/install-marquez-ocp.sh
```

The script:
1. Checks EDB is healthy and `marquez-postgres-app` secret exists.
2. Creates a key-remapping secret `marquez-db-secret` (bridges EDB key `password`
   → Helm chart key `marquez-db-password`).
3. Sparse-clones the Marquez `chart/` from GitHub `main`.
4. Runs `helm upgrade --install` with `--wait --timeout 5m`.
5. Applies the Routes.

### Dry-run preview

```bash
bash openlineage-marquez/ocp/install-marquez-ocp.sh --dry-run
```

---

## Day-2 operations

### Check pod health

```bash
oc get pods -n cpd-instance | grep marquez
# Expected:
# marquez-<hash>         1/1  Running
# marquez-postgres-1     1/1  Running
# marquez-web-<hash>     1/1  Running
```

### View API logs

```bash
oc logs -n cpd-instance -l app.kubernetes.io/name=marquez,app.kubernetes.io/component=marquez --tail=50
```

### Restart Marquez API

```bash
oc rollout restart deployment/marquez -n cpd-instance
oc rollout status  deployment/marquez -n cpd-instance
```

### Upgrade to a newer version

Edit `openlineage-marquez/ocp/02-marquez-values-ocp.yaml` — update both
`marquez.image.tag` and `web.image.tag` to the new version, then:

```bash
bash openlineage-marquez/ocp/install-marquez-ocp.sh   # runs helm upgrade
```

### Uninstall (keeps EDB data)

```bash
bash openlineage-marquez/ocp/install-marquez-ocp.sh --uninstall

# To also remove the EDB cluster and its PVC:
oc delete cluster.postgresql.k8s.enterprisedb.io marquez-postgres -n cpd-instance
oc delete pvc -n cpd-instance -l cnpg.io/cluster=marquez-postgres
```

### Check EDB PostgreSQL

```bash
# Cluster status
oc get cluster.postgresql.k8s.enterprisedb.io marquez-postgres -n cpd-instance

# Connect to DB (from any pod with psql, or a debug pod)
oc exec -it marquez-postgres-1 -n cpd-instance -- psql -U marquez marquez

# Get DB credentials
oc get secret marquez-postgres-app -n cpd-instance -o yaml
```

---

## Sending OpenLineage events from Spark

### Option A — Per-job conf (recommended)

When submitting via `POST /lakehouse/api/v3/spark_engines/{engine_id}/applications`,
add to the `"conf"` object:

```json
"conf": {
  "spark.jars.packages": "io.openlineage:openlineage-spark_2.12:1.32.0",
  "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
  "spark.openlineage.transport.type": "http",
  "spark.openlineage.transport.url": "http://marquez.cpd-instance.svc.cluster.local:5000",
  "spark.openlineage.transport.endpoint": "/api/v1/lineage",
  "spark.openlineage.namespace": "watsonxdata-spark",
  "spark.openlineage.parentJobName": "my-job-name"
}
```

The `openlineage-spark` JAR is fetched from Maven Central at job start. If the Spark
engine has no internet access, copy the JAR to the engine home bucket and reference it
via `spark.jars = s3a://<bucket>/jars/openlineage-spark_2.12-1.32.0.jar`.

### Option B — Cluster-wide (all jobs, requires AE restart)

See `openlineage-marquez/ocp/04-spark-openlineage-integration.yaml` for the
`oc patch analyticsengine` command.  
**Confirm with the user before applying** — this restarts the AnalyticsEngine controller.

---

## Sending events from the workstation (dbt / Python)

Set in `.env`:

```bash
OPENLINEAGE_URL=https://marquez-api.apps.watson.ibmas-zocp-techcluster.org
```

Then run:

```bash
python scripts/emit_openlineage_events.py
```

The script posts OpenRunEvent/OpenCompleteEvent JSON to `/api/v1/lineage`.

---

## Verifying lineage data

```bash
# List namespaces
curl -sk https://marquez-api.apps.watson.ibmas-zocp-techcluster.org/api/v1/namespaces | python3 -m json.tool

# List jobs in a namespace
curl -sk "https://marquez-api.apps.watson.ibmas-zocp-techcluster.org/api/v1/namespaces/watsonxdata-spark/jobs" | python3 -m json.tool

# List datasets
curl -sk "https://marquez-api.apps.watson.ibmas-zocp-techcluster.org/api/v1/namespaces/watsonxdata-spark/datasets" | python3 -m json.tool
```

The Web UI (https://marquez-web.apps.watson.ibmas-zocp-techcluster.org) provides a
visual lineage graph — no login required.

![Marquez Spark lineage graph — watsonxdata-spark namespace](assets/images/screenshots/marquez-spark-lineage.png)
/// caption
Marquez `watsonxdata-spark` namespace after a full Spark medallion run. Mode: **Table Level**, Depth: **2**. Purple nodes = datasets (bronze/silver/gold tables with visible column schemas); teal gear nodes = Spark write jobs; arrows = data flow direction. The job name prefix `watsonxdata_medallion_demo.atomic_replac…` reflects the Spark job name configured via `spark.openlineage.parentJobName`.
///

---

## Known quirks & decisions

| Item | Detail |
|---|---|
| **EDB secret key mismatch** | The Marquez Helm chart's `_helpers.tpl` hardcodes the expected secret key as `marquez-db-password`, but the EDB operator stores it under `password`. `install-marquez-ocp.sh` Step 1b creates a thin wrapper secret `marquez-db-secret` that remaps the key. |
| **`marquez-web` Service port** | The chart generates `service.port: 5000` targeting `containerPort: 3000`. The Route in `03-routes.yaml` targets `targetPort: http` which resolves to 3000 — correct. |
| **SCC** | Both Marquez pods run under `restricted-v2` (default for `cpd-instance`). The chart has no explicit `securityContext` so OpenShift assigns a random UID from the namespace range — this is correct. |
| **No auth on API** | Marquez 0.51.x has no built-in auth. Access is gated by the OpenShift Route (edge TLS). In-cluster access is unrestricted — any pod in `cpd-instance` can POST lineage events. |
| **EDB sizing** | `instances: 1`, `10Gi` — adequate for demo/development. For production use `instances: 2` and `20Gi`. |
| **Maven Central access** | Spark engine must reach `repo1.maven.org` to download the `openlineage-spark` JAR at job start. If egress is blocked, pre-stage the JAR in the engine bucket. |

---

## Resource summary

```
Namespace: cpd-instance
  Deployments:
    marquez         1/1  (CPU: 250m–1, Mem: 512Mi–1Gi)
    marquez-web     1/1  (CPU: 100m–500m, Mem: 256Mi–512Mi)
  StatefulSet (EDB):
    marquez-postgres-1  1/1  (CPU: 250m–1, Mem: 512Mi–1Gi)
  PVC:
    marquez-postgres-1  10Gi  managed-nfs-storage
  Services:
    marquez              ClusterIP  :5000 (API)
    marquez-web          ClusterIP  :5000→3000 (Web)
    marquez-postgres-rw  ClusterIP  :5432
    marquez-postgres-r   ClusterIP  :5432
    marquez-postgres-ro  ClusterIP  :5432
  Routes (edge TLS):
    marquez-api  → marquez:5000
    marquez-web  → marquez-web:3000
```
