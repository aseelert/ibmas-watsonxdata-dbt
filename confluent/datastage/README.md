<!--
=============================================================================
 README.md — Confluent GOLD via IBM DataStage (the no-code ETL alternative)

 Location  : confluent/datastage/README.md
 Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
 Project   : watsonx.data · dbt · Spark · Confluent medallion demo
 Author    : Alexander Seelert — IBM Customer Success Engineer
 Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.

 Changelog :
   v1.0 (2026-06-26) — Initial version. Explains the DataStage gold engine, the
     flow design, prerequisites, and how it compares to the Spark gold engine.
=============================================================================
-->

# Confluent GOLD with IBM DataStage

This folder holds the **DataStage** way to build the Confluent gold marts. It is
the *alternative engine* to the default Spark gold job. Both produce the **exact
same** `confluent_demo_gold` tables — the whole point is to show a customer:

> "Here is the same business result, once as **code-first Spark SQL** and once as
> **visual, no-code DataStage ETL** — same silver in, same gold out, same numbers."

You pick the engine with one env var (see `confluent/NAMING.md`):

```bash
CONFLUENT_GOLD_ENGINE=spark        # default — watsonx.data Spark job runs the gold SQL
CONFLUENT_GOLD_ENGINE=datastage    # this folder — a DataStage flow runs the same logic
```

---

## Where DataStage sits in the pipeline

```
 4 seed CSVs ─► Kafka (raw topics) ─► Flink SQL ─► confluent_demo_silver  (Iceberg)
                                                          │
                                                          │  ← gold engine reads silver
                                                          ▼
                          ┌──────────────── CONFLUENT_GOLD_ENGINE ───────────────┐
                          │                                                       │
                    spark │                                                       │ datastage
                          ▼                                                       ▼
              watsonx.data Spark job                          IBM DataStage flow (THIS FOLDER)
                          │                                                       │
                          └───────────────► confluent_demo_gold ◄────────────────┘
                                  confluent_gold_daily_sales
                                  confluent_gold_category_performance
                                  confluent_gold_customer_360
```

The **silver** layer is always written by **Flink** (`silver_jobs.sql`). Only the
**gold** step swaps engines. The grain and metrics are defined once, by the dbt
models in `models/gold/` — they are the source of truth, and this DataStage flow
copies their logic verbatim.

---

## The three gold marts (must match dbt + Spark exactly)

| Mart                                   | Grain                       | Logic (from `models/gold/*.sql`) |
|----------------------------------------|-----------------------------|----------------------------------|
| `confluent_gold_daily_sales`           | one row per (`order_date`, `category`) | `where status='completed'`, then `count(distinct order_id)`, `sum(quantity)`, `cast(sum(net_amount) as decimal(14,2))` |
| `confluent_gold_category_performance`  | one row per `category`      | roll-up of `daily_sales`: `sum(order_count)`, `sum(units_sold)`, `sum(net_revenue)`, `net_revenue/nullif(units_sold,0)` |
| `confluent_gold_customer_360`          | one row per customer        | per-status `count(distinct …)` + `lifetime_value`, **LEFT** joined to customers so 0-order customers still appear (0-filled) |

> Reconciliation compares the **set of rows**, not their order — the dbt marts
> intentionally carry no `ORDER BY`. Keep the casts and the `count(distinct …)`
> exactly as-is; they are part of the parity contract.

---

## Flow design (`confluent_gold_flow.json`)

`confluent_gold_flow.json` is a parameterized **pipeline-flow v3** graph (the
format the CP4D DataStage REST API accepts). The fully-wired mart is
`confluent_gold_daily_sales`:

```
 [Connector: confluent_silver_sales_enriched]   ← reads watsonx.data via a connection asset
        │  SELECT order_date, category, order_id, quantity, net_amount, status
        ▼
 [Filter: status = 'completed']
        ▼
 [Aggregator: group by order_date, category]
        │  count_distinct(order_id) → order_count
        │  sum(quantity)            → units_sold
        │  sum(net_amount)::dec(14,2) → net_revenue
        ▼
 [Connector: confluent_gold_daily_sales]        ← writes confluent_demo_gold (replace)
```

The other two marts are present as **clearly-labelled placeholder nodes** with
their exact target table, source SQL file, and the select/join they need —
ready to be wired the same way (the `customer_360` one calls out the LEFT-outer
Join + coalesce that keeps zero-order customers).

### Placeholders the script fills in from `.env`

| Placeholder           | Env var                          | Default                  |
|-----------------------|----------------------------------|--------------------------|
| `@@PROJECT_ID@@`      | `WXD_DATASTAGE_PROJECT_ID`       | (required)               |
| `@@CATALOG@@`         | `WXD_CATALOG`                    | `iceberg_data`           |
| `@@SILVER_SCHEMA@@`   | `CONFLUENT_SILVER_SCHEMA`        | `confluent_demo_silver`  |
| `@@GOLD_SCHEMA@@`     | `CONFLUENT_GOLD_SCHEMA`          | `confluent_demo_gold`    |
| `@@CONNECTION_REF@@`  | `WXD_DATASTAGE_CONNECTION_REF`   | (placeholder + warning)  |
| `@@CONNECTION_NAME@@` | `WXD_DATASTAGE_CONNECTION_NAME`  | "watsonx.data Presto connection" |

Nothing is hardcoded — every host / schema / id comes from `.env`.

---

## Prerequisites

1. **A live CP4D cluster with the DataStage cartridge.** The DataStage flows API
   (`/data_intg/v3/data_intg_flows`) does not exist without it. This template was
   authored offline and **cannot be validated here** — see the warning below.
2. **The CP4D project must already exist:** `ibmas-ingest-demo`
   (`WXD_DATASTAGE_PROJECT_ID = 2d2415ea-71b5-4215-a7b6-b32a4889611e`).
3. **A watsonx.data connection asset inside that project.** DataStage reaches
   Iceberg through a *connection asset*, not raw credentials. Create one (CP4D →
   the project → New asset → Connection → "IBM watsonx.data" / Presto) pointing at
   the same `WXD_HOST` / `WXD_PORT` / `WXD_API_KEY` the dbt and Spark paths use.
   Copy its **asset guid** into `WXD_DATASTAGE_CONNECTION_REF` and its display
   name into `WXD_DATASTAGE_CONNECTION_NAME`.
4. **The gold schema exists:** `confluent_demo_gold` (the Spark path / prep step
   creates it; otherwise `CREATE SCHEMA iceberg_data.confluent_demo_gold` once).
5. **The Flink silver tables are populated** (`confluent_demo_silver.confluent_silver_*`).
6. Valid auth in `.env`: `WXD_CPD_HOST`, `WXD_CPD_USERNAME` (`cpadmin`),
   `WXD_API_KEY` (or `WXD_CPD_PASSWORD`). Sanity-check with
   `python scripts/get_token.py`.

---

## How to use it

```bash
# 0. (once) sanity-check auth
python scripts/get_token.py

# 1. Preview the exact request — DRY RUN, no network call (this is the default):
python confluent/scripts/create_datastage_flow.py

# 2. Create the flow on the live cluster:
python confluent/scripts/create_datastage_flow.py --apply

# 3. Create it, compile it, and trigger a job run:
python confluent/scripts/create_datastage_flow.py --apply --run
```

`create_datastage_flow.py` authenticates exactly like `scripts/get_token.py`
(API key first, password fallback), renders the template, and POSTs it. Open the
created flow in the DataStage canvas afterwards to confirm the connector bindings
resolved against your CP4D version, then run it.

> ⚠️ **NEEDS A LIVE DATASTAGE SERVICE — NOT VALIDATED OFFLINE.**
> This flow JSON and script are a best-effort, well-documented starting point.
> The pipeline-flow schema is correct in shape, but exact connector `op` names
> and property keys vary by CP4D release. Expect to open the flow once in the
> canvas so the service can normalize it before the first successful run. The
> **business logic** (the SQL, copied from the dbt gold models) is the part that
> is guaranteed correct and must not be changed.

---

## DataStage vs the Spark gold engine

| Aspect            | Spark gold (default)                       | DataStage gold (this folder)                  |
|-------------------|--------------------------------------------|-----------------------------------------------|
| Authoring style   | Code-first Spark SQL                        | Visual, no-code drag-and-drop ETL canvas      |
| Runtime           | watsonx.data Spark engine                   | DataStage PX parallel engine in CP4D          |
| Reads             | `confluent_demo_silver` (Iceberg)           | `confluent_demo_silver` via a connection asset |
| Writes            | `confluent_demo_gold` marts                 | the **same** `confluent_demo_gold` marts      |
| Audience appeal   | data engineers who live in SQL/notebooks    | enterprise ETL teams who standardize on GUI   |
| Result            | identical numbers — that's the whole demo   | identical numbers — that's the whole demo     |

Same four CSVs, same silver, same gold. The only thing that changes is *who does
the work* — which is exactly the story this alternative is meant to tell.
