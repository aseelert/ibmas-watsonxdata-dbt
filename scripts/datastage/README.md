# DataStage medallion path (bronze → silver → gold)

A **fourth, interchangeable medallion path** next to dbt / Spark / cpdctl, built as
**five IBM DataStage** flows on Cloud Pak for Data **5.3.4**. Every transformation is
the *exact* dbt SQL/logic, pushed down to the **watsonx.data Presto** engine through
**one connection** (`ibmas-presto`), with real DataStage **Transformer** and **Join**
stages doing the cast/clean/enrich work visually instead of as raw SQL pushdown.

```
CSV already landed as Iceberg in  iceberg_data.dbt_demo_raw        (on MinIO)
   │  ds_medallion_bronze_v2         raw + ingest metadata
   ▼  iceberg_data.datastage_demo_bronze_v2.*
   │  ds_medallion_silver_clean_v2   cast / filter + Transformer (trim/lower/upper)
   ▼  iceberg_data.datastage_demo_silver_v2.* (clean tables)
   │  ds_medallion_silver_enrich_v2  3 Join stages + Transformer → silver_sales_enriched
   ▼  iceberg_data.datastage_demo_silver_v2.silver_sales_enriched
   │  ds_medallion_gold_daily_v2     gold_daily_sales
   ▼  iceberg_data.datastage_demo_gold_v2.gold_daily_sales
   │  ds_medallion_gold_marts_v2     gold_category_performance + gold_customer_360
   ▼  iceberg_data.datastage_demo_gold_v2.*
```

Run order is **bronze → silver_clean → silver_enrich → gold_daily → gold_marts**
(each flow reads the physical tables the previous flow wrote). DataStage can run
independent branches within one flow concurrently, so dbt dependencies that cross a
model boundary are split into their own flow asset rather than folded into a single
flow.

## Files

| File | Purpose |
|---|---|
| `create_medallion_flows_v2.py` | Builds the pipeline-flow **v3** JSON for all 5 flows (bronze, silver clean, silver enrich, gold daily, gold marts) with real `Transformer`/`PxJoin` stages, creates the target schemas, POSTs the flows, and — with `--run` — recreates each target table, compiles, and executes the flows in order. Also runs a Presto parity check vs the dbt tables (`--verify`). |
| `flows/ds_medallion_*_v2.json` | The generated, version-controllable flow definitions. |

`_archive/create_medallion_flows.py` and `_archive/ds_flow_lib.py` are the **v1**
implementation (3 flows, SQL-pushdown-only, no Transformer/Join stages, no compile+run
path). They are superseded by v2 and kept only for reference —
[`docs/datastage-medallion.md`](../../docs/datastage-medallion.md) no longer documents
them, and they are not otherwise wired into any script or doc.

## Usage

```bash
source .venv/bin/activate          # needs requests, python-dotenv, presto-python-client
# 1) build the JSON only
python scripts/datastage/create_medallion_flows_v2.py --build
# 2) prove the SQL/logic matches dbt (row counts + gold value sums) — no DataStage runtime needed
python scripts/datastage/create_medallion_flows_v2.py --verify
# 3) create the target schemas + the 5 flows in the ibmas-ingest-demo project
python scripts/datastage/create_medallion_flows_v2.py --create
# 4) create, then compile + recreate targets + run the flows in order
python scripts/datastage/create_medallion_flows_v2.py --create --run
```

Auth/env come from `.env` (the same `WXD_*` vars dbt and Presto use). A CPD bearer
token is minted from `WXD_API_KEY`.

Created flow asset ids live in project `ibmas-ingest-demo`:
`ds_medallion_bronze_v2`, `ds_medallion_silver_clean_v2`,
`ds_medallion_silver_enrich_v2`, `ds_medallion_gold_daily_v2`,
`ds_medallion_gold_marts_v2`. Open them in
**Projects → ibmas-ingest-demo → Assets → DataStage flows**.

## Parity (verified)

`--verify` re-points each model's SQL at the populated `dbt_demo_*` tables and diffs
against the dbt-built tables. All 13 models match row-for-row, and the three gold
revenue/lifetime-value sums reconcile to the penny (`$87,509.85`).

## Do I need a "DataStage SDK"? Does the MCP build flows?

**No SDK, and no — the MCP does not author flows.** Findings:

- The **watsonx.data *intelligence* MCP** (`mcp__ibm-watsonx-data-intelligence__*`)
  governs catalog/connections, metadata import & enrichment, glossary, **data-quality
  rules**, lineage, and data products. It has **no DataStage flow-authoring tool**. It
  *does* indirectly create DataStage flows as a side effect of `create_data_quality_rule_*`
  (those are the 57 `DataStage flow of data rule …` assets already in the project) — but
  there is no "create ETL flow" verb.
- A DataStage flow is just **pipeline-flow v3 JSON** stored as a `data_intg_flow` CAMS
  asset. We create it with the plain **Watson Data REST API**:
  `POST /data_intg/v3/data_intg_flows?project_id=…&data_intg_flow_name=…` with body
  `{"pipeline_flows": <doc>}`. No `ibm-datastage` Python package is required (cpdctl's
  optional `dsjob` plugin is another option, but it is **not installed** here and is not
  needed).

## Connector contract used (CPD 5.3.4, reverse-engineered from existing flows)

watsonx.data Presto connector node: `op:"lakehouse"`, `type:"binding"`,
`connection.ref` = the `ibmas-presto` connection id.

- **source** — `properties.read_mode:"select"` + `properties.select_statement:"<SQL>"`
  (bronze/silver clean, silver enrich pre-Transformer) or `properties.read_mode:"general"`
  + `schema_name`/`table_name` feeding a `Transformer`/`PxJoin` stage.
- **target** — `properties.table_action:"append"` (with `--run` recreating an empty
  Iceberg table via Presto immediately beforehand) + `catalog_name` / `schema_name` /
  `table_name`.

## Caveat — design-time vs runtime

Flow **creation** (design-time, CAMS) works and is verified by round-trip. **Compiling
and running** a flow needs the DataStage **px-runtime** to be active; when the runtime
instance is not started, `POST …/compile` returns `500` for every flow — an environment
state, not a defect in these flows. Start/scale the DataStage instance in CPD, then
`--create --run` compiles and runs **bronze → silver_clean → silver_enrich → gold_daily
→ gold_marts** in order. Until then, `--verify` proves the logic on the live Presto
engine.
