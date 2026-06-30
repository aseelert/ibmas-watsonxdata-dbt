# DataStage medallion path (bronze → silver → gold)

A **fourth, interchangeable medallion path** next to dbt / Spark / cpdctl, built as
three **IBM DataStage** flows on Cloud Pak for Data **5.3.4**. Every transformation is
the *exact* dbt SQL, pushed down to the **watsonx.data Presto** engine through **one
connection** (`ibmas-presto`). DataStage just orchestrates read → write.

```
CSV already landed as Iceberg in  iceberg_data.dbt_demo_raw   (on MinIO)
   │  ds_medallion_bronze   raw + ingest metadata
   ▼  iceberg_data.datastage_demo_bronze.*
   │  ds_medallion_silver   cast / clean / filter / inner-join (silver_sales_enriched)
   ▼  iceberg_data.datastage_demo_silver.*
   │  ds_medallion_gold     business aggregates (daily_sales, category_perf, customer_360)
   ▼  iceberg_data.datastage_demo_gold.*
```

Run order is **bronze → silver → gold** (each layer reads the physical tables the
previous layer wrote). Within a flow every stage reads only a *prior* layer, so the
stages are safe to run in parallel — `silver_sales_enriched` inlines the four cleaning
CTEs over bronze instead of reading the sibling silver tables, and
`gold_category_performance` reads `silver_sales_enriched` directly instead of
`gold_daily_sales`. Both rewrites are provably equivalent (see parity check).

## Files

| File | Purpose |
|---|---|
| `ds_flow_lib.py` | Builds pipeline-flow **v3** JSON: one `lakehouse` (watsonx.data Presto) source→target pair per model, with custom-SQL pushdown on the source and `table_action=replace` on the target. |
| `create_medallion_flows.py` | Defines all 13 models, builds/writes JSON, creates the target schemas, POSTs the flows, and runs a Presto parity check vs the dbt tables. |
| `flows/ds_medallion_{bronze,silver,gold}.json` | The generated, version-controllable flow definitions. |

## Usage

```bash
source .venv/bin/activate          # needs requests, python-dotenv, presto-python-client
# build the JSON only
python scripts/datastage/create_medallion_flows.py --build
# prove the SQL matches dbt (row counts + gold value sums) — no DataStage runtime needed
python scripts/datastage/create_medallion_flows.py --verify
# create the schemas + the 3 flows in the ibmas-ingest-demo project
python scripts/datastage/create_medallion_flows.py --create
```

Auth/env come from `.env` (the same `WXD_*` vars dbt and Presto use). A CPD bearer
token is minted from `WXD_API_KEY`.

Created flow asset ids (project `ibmas-ingest-demo` = `2d2415ea-…611e`):
`ds_medallion_bronze`, `ds_medallion_silver`, `ds_medallion_gold`. Open them in
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
- **target** — `properties.table_action:"replace"` + `catalog_name` / `schema_name` / `table_name`

## Caveat — design-time vs runtime

Flow **creation** (design-time, CAMS) works and is verified by round-trip. **Compiling
and running** a flow needs the DataStage **px-runtime** to be active; on this instance
`POST …/compile` currently returns `500` for *every* flow (including the pre-existing
`DS-merge` and the data-rule flows), i.e. the runtime instance is not started — an
environment state, not a defect in these flows. Start/scale the DataStage instance in
CPD, then compile and run **bronze → silver → gold** in order (or create a DataStage job
per flow). Until then, `--verify` proves the logic on the live Presto engine.
