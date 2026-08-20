---
name: ibmas-ikc-platform-ops-runbook
description: "Use for IBM Knowledge Catalog / WKC PLATFORM operations — CR health, sizing, DataStage/pxruntime status, metadata-import/enrichment job troubleshooting, and REST contracts. This is ops, NOT governance content authoring (business terms/data classes CSV import is the separate ikc-governance skill — do not duplicate it). Triggers on phrases like: 'is IKC healthy', 'WKC CR', 'enrichment job failed', 'MDI 404', 'DataStage flow REST', 'IKC sizing', 'pxruntime status'."
---

# IKC / WKC Platform Ops Runbook

**Scope note**: this skill is about the *platform* (CRs, pods, REST contracts, job
failures) — CSV-based business-term/data-class authoring is the existing `ikc-governance`
skill; don't merge the two.

## CR reference (live on this cluster, ns `cpd-instance`)

| CR | Name | Version |
|---|---|---|
| `wkc.wkc.cpd.ibm.com` | `wkc-cr` | 5.3.17 |
| `datastages.ds.cpd.ibm.com` | `datastage` | 5.3.6 |
| `pxruntimes.ds.cpd.ibm.com` | `ds-px-default`, `ibmas-datastage` | 5.3.6 |

Related CRDs on this cluster you may also need: `dataquality.wkc.cpd.ibm.com`,
`enrichment.wkc.cpd.ibm.com`, `glossary.wkc.cpd.ibm.com`, `metadataimports.wkc.cpd.ibm.com`,
`profiling.wkc.cpd.ibm.com`, `knowledgegraph.wkc.cpd.ibm.com`, `neo4jclusters.neo4j.cpd.ibm.com`
(WKC's knowledge-graph backing store).

```bash
oc get wkc.wkc.cpd.ibm.com -A -o wide
oc get datastages.ds.cpd.ibm.com,pxruntimes.ds.cpd.ibm.com -A -o wide
oc get pods -n cpd-instance -l app.kubernetes.io/instance=wkc-cr    # confirm label before trusting
```

## Known capability limits (verified on this cluster, re-check per environment)

- **No Semantic automation layer** → MDE objectives `assign_terms`, `semantic_expansion`,
  `data_search` fail the whole job with "Semantic automation layer is not available."
  Supported: `profile`, `analyze_quality`, `analyze_relationships`, `dq_gen_constraints`.
  Data-**class** assignment works during profiling; business-**term** assignment does not.
- **MinIO metadata import unsupported**: "no reader for datasource type minio" — catalog
  MinIO-backed data via the Presto connection instead.
- **No project-global DQ-output-DB API** — DQ results land in a DB only via per-rule output.

## Root cause: enrichment 404 on profile

If profiling 404s from `wdp-connect-connection`, check whether the data asset's
`discovered_asset.connection_id` points at a connection that was **deleted and
recreated** (new connection = new id, old assets still reference the dead one).
**Fix discipline: on re-provision, EDIT the connection in place — never delete+recreate**,
it orphans every asset that references it. If it already happened, re-run the metadata
import so assets relink to the live connection.

## Reverse-engineered REST contracts (no public doc covers these)

- **MDI create**: `POST /v2/metadata_imports?project_id&job_name&create_job=true`; run via
  `GET` the job id off `entity.job_id`, then `POST /v2/jobs/{job}/runs`.
- **MDE create/patch**: must use the **non-legacy** path —
  `POST|PATCH /metadata_enrichment/v3/metadata_enrichment_assets` (the legacy `_legacy_assets`
  path 500s). `data_scope.container_assets.metadata_imports=[mdi]`;
  `governance_scope` = the categories that scope term/data-class assignment.
- **DQ rule create**: `POST /data_quality/v3/projects/{pid}/rules`
  `{name, dimension.id, input.sql{connection.id, select_statement}}`; execute via
  `POST .../rules/{id}/execute` (runs as a DataStage flow under the hood). To output to a
  DB: `output.columns=[{type:"metric", name, metric:"rule_name"|"system_date"}]` (SQL rules
  can't reference input columns), `output.database.location={connection.id, schema_name,
  table_name}`, `records_type=failing_records`. The output connection **must be in the
  project** — copy a platform connection in first via
  `POST /v2/connections?project_id&persist=true&test=true` with
  `{ref_catalog_id, ref_asset_id}`.
- **DataStage flow create**: `POST /data_intg/v3/data_intg_flows?project_id=&data_intg_flow_name=`
  body `{"pipeline_flows": <pipeline-flow v3 doc>}` → 201, no DataStage SDK needed. The
  pipeline JSON itself is an **attachment** on the asset (`name:"data_intg_flows"`), not
  in the v3 GET body — read via `/v2/assets/{id}` → attachment id → attachment url. The
  watsonx.data Presto DataStage connector op is `lakehouse` (type `binding`); source uses
  `read_mode:"select"` + `select_statement`, target uses `table_action:"replace"` +
  `catalog_name`/`schema_name`/`table_name`.
- `/data_intg/v3/.../compile` and its `DELETE` can 500 if `pxruntime` is scaled down — use
  `/v2/assets/{id}` `DELETE` (204) instead for flow deletion; flow **creation** (CAMS-side)
  works even when compile/run needs the DataStage instance actually started.

## Safety tiering for this skill

- **Auto-run**: all `GET`/`oc get`/`describe` calls, job-status polling, connection-id
  cross-checks.
- **Confirm first**: any CR `spec` patch/sizing change, deleting a connection (risk of
  re-orphaning assets — edit in place instead), executing a DQ rule or MDE job against
  production-scale data without the user confirming scope.
