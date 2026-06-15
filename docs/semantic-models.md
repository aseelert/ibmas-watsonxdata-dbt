# Semantic Models

!!! abstract "What this page is about"
    A **semantic model** is a structured description of a dbt model that tells BI tools and
    AI assistants *what* the columns mean — not just their names, but whether they are
    dimensions to filter by, measures to add up, or keys to join on. This demo ships two
    semantic models so you can see how the concept works.

## Why semantic models exist

Imagine you build a beautiful `gold_daily_sales` table. A data analyst opens it and sees columns
called `order_count`, `units_sold`, and `net_revenue`. They can probably guess what those mean.
But how does a BI tool know that `net_revenue` should be *summed*, not *averaged*? How does it
know that `order_date` is a time dimension you can drill by week or month? How does it know
that `category` is a label you can group by?

A semantic model answers all those questions in one YAML file. It is the difference between
"here is some data" and "here is a *business concept* with well-defined metrics".

```
Raw CSV  →  Bronze  →  Silver  →  Gold TABLE / VIEW
                                       ↑
                               semantic_models.yml  ← describes the business meaning
                                       ↓
                             BI tool / MetricFlow query
```

## The two semantic models in this demo

Both are defined in `models/semantic_models.yml`.

### `daily_sales` — built on `gold_daily_sales`

This is the **presentation layer**: a pre-aggregated, partitioned table that BI dashboards
can query without ever touching raw data.

| Field | Type | Column | What it means |
| --- | --- | --- | --- |
| `order_date` | **Time dimension** | `order_date` | Drill by day / week / month |
| `category` | **Categorical dimension** | `category` | Filter or group by product category |
| `total_orders` | **Measure** (SUM) | `order_count` | Count of completed orders |
| `total_units_sold` | **Measure** (SUM) | `units_sold` | Total product units |
| `total_net_revenue` | **Measure** (SUM) | `net_revenue` | Revenue after discounts |

### `sales_orders` — built on `silver_sales_enriched`

This is the **flexible fact**: one row per order line item, all four entities already joined.
Use this for ad-hoc analysis when you need to slice by country, payment method, or status.

| Field | Type | Column | What it means |
| --- | --- | --- | --- |
| `order_date` | **Time dimension** | `order_date` | Full date resolution |
| `category` | **Categorical dimension** | `category` | Product category |
| `customer_country` | **Categorical dimension** | `customer_country` | Buyer's country |
| `payment_method` | **Categorical dimension** | `payment_method` | card / paypal / bank_transfer |
| `status` | **Categorical dimension** | `status` | completed / returned / cancelled / pending |
| `order_id` | **Entity** (foreign key) | `order_id` | Join key to the order header |
| `customer_id` | **Entity** (foreign key) | `customer_id` | Join key to the customer dimension |
| `product_id` | **Entity** (foreign key) | `product_id` | Join key to the product dimension |
| `net_revenue` | **Measure** (SUM) | `net_amount` | Revenue after discounts |
| `gross_revenue` | **Measure** (SUM) | `gross_amount` | Revenue before discounts |
| `units_sold` | **Measure** (SUM) | `quantity` | Units per line item |

## The YAML that defines them

```yaml
semantic_models:
  - name: daily_sales
    model: ref('gold_daily_sales')
    defaults:
      agg_time_dimension: order_date
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
      - name: category
        type: categorical
    measures:
      - name: total_net_revenue
        agg: sum
        expr: net_revenue
```

The `agg: sum` on `total_net_revenue` is the key part: it tells MetricFlow "when you need
revenue, sum the `net_revenue` column". A BI tool querying this semantic model never has to
write `SUM(net_revenue)` itself — it just asks for the `total_net_revenue` measure.

## How dbt validates the semantic models

```bash
cd /path/to/project
source .venv/bin/activate
dbt parse   # validates YAML structure including semantic models
```

`dbt parse` reads the YAML and checks that every `expr:` column exists in the referenced
model, that time dimensions have a `time_granularity`, and that measure aggregations are
valid. No extra package is needed for validation.

## How to query the semantic layer (MetricFlow)

To actually run metric queries you need the MetricFlow CLI:

```bash
pip install dbt-metricflow
mf validate-configs   # checks semantic models against the live warehouse
mf query --metrics total_net_revenue --group-by order_date__month
```

!!! note "MetricFlow and this demo"
    `dbt-metricflow` is not in this project's `requirements.txt` because it requires a
    supported semantic layer backend. The semantic models in `models/semantic_models.yml` are
    validated by `dbt parse` and serve as the source-of-truth business definitions — even
    without MetricFlow running. Any BI tool that understands the dbt semantic layer spec
    (Lightdash, Cube, Superset with dbt integration) can read them directly.

## Where this fits in the medallion layers

```
Raw (CSV seeds)
    │
    ▼
Bronze (typed copies)
    │
    ▼
Silver (silver_sales_enriched ←── semantic model: sales_orders)
    │
    ▼
Gold  (gold_daily_sales ←── semantic model: daily_sales)
       gold_category_performance  (view on gold_daily_sales — MV ready when Presto Iceberg supports it)
       gold_customer_360          (view on silver_sales_enriched)
```

The semantic models sit at the gold and silver layers — exactly where business logic lives.
