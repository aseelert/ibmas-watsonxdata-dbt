<section class="hero">
  <span class="eyebrow">Medallion Architecture</span>
  <h1>From four CSV files to governed gold marts — traced column by column</h1>
  <p>
    This page shows the full <strong>medallion lineage</strong> of the demo: how the raw
    CSV files flow through bronze, silver, and gold, and exactly which column becomes
    which. Use it to explain to a customer how a single field — say a discount percentage —
    travels from a spreadsheet cell into <code>net_revenue</code> on a dashboard.
  </p>
</section>

<div class="brand-strip" markdown>
<span class="brand-label">Built on</span>
![IBM](assets/images/ibm.svg)
![watsonx.data](assets/images/watsonx.svg)
![dbt](assets/images/dbt.svg)
![Apache Spark](assets/images/spark.svg)
![Apache Iceberg](assets/images/iceberg.svg)
</div>

## The Four Medallion Layers

<p class="lede">Each layer has one job. Data only ever moves left to right, and every layer keeps the one before it intact — so you can always trace a number back to the file it came from.</p>

<div class="layer-grid">
  <div class="card raw">
    <span class="layer-tag">Raw</span>
    <h3>Source files</h3>
    <p>The original CSV exports. The true landing zone — strings only, nothing cleaned. Kept for full traceability.</p>
  </div>
  <div class="card bronze">
    <span class="layer-tag">Bronze</span>
    <h3>Ingested copy</h3>
    <p>First managed Iceberg tables. Same columns as the source, plus ingest metadata (when, by what, from which file, which batch).</p>
  </div>
  <div class="card silver">
    <span class="layer-tag">Silver</span>
    <h3>Clean &amp; typed</h3>
    <p>Strings become real dates, integers, and decimals. Trimmed, lower-cased, validated, deduplicated business entities.</p>
  </div>
  <div class="card gold">
    <span class="layer-tag">Gold</span>
    <h3>Business marts</h3>
    <p>Views that answer questions: daily sales by category, and a customer 360 with lifetime value. What dashboards read.</p>
  </div>
</div>

!!! info "How to read the object types"
    Every box in this demo is one of three things:
    <span class="obj csv">CSV</span> a flat file in object storage &nbsp;·&nbsp;
    <span class="obj table">TABLE</span> a physical Iceberg table &nbsp;·&nbsp;
    <span class="obj view">VIEW</span> a logical query that runs on read.
    Raw → bronze → silver are **tables**; gold is **views**.

## End-to-End Lineage

This is the whole pipeline at a glance. The four sources fan in through the layers and converge into the two gold marts.

```mermaid
flowchart LR
  classDef csv    fill:#edf5ff,stroke:#0f62fe,color:#161616;
  classDef bronze fill:#fbf1e8,stroke:#b46d3c,color:#161616;
  classDef silver fill:#f1f2f4,stroke:#6f7079,color:#161616;
  classDef gold   fill:#fcf4d6,stroke:#b28600,color:#161616;

  subgraph RAW["RAW · CSV files"]
    direction TB
    c0["raw_customers.csv"]:::csv
    p0["raw_products.csv"]:::csv
    o0["raw_orders.csv"]:::csv
    i0["raw_order_items.csv"]:::csv
  end

  subgraph BRZ["BRONZE · Iceberg tables"]
    direction TB
    c1["bronze_customers"]:::bronze
    p1["bronze_products"]:::bronze
    o1["bronze_orders"]:::bronze
    i1["bronze_order_items"]:::bronze
  end

  subgraph SLV["SILVER · Iceberg tables"]
    direction TB
    c2["silver_customers"]:::silver
    p2["silver_products"]:::silver
    o2["silver_orders"]:::silver
    i2["silver_order_items"]:::silver
  end

  subgraph GLD["GOLD · views"]
    direction TB
    g1["gold_daily_sales"]:::gold
    g2["gold_customer_360"]:::gold
  end

  c0 --> c1 --> c2
  p0 --> p1 --> p2
  o0 --> o1 --> o2
  i0 --> i1 --> i2

  o2 --> g1
  i2 --> g1
  p2 --> g1

  c2 --> g2
  o2 --> g2
  i2 --> g2
  p2 --> g2
```

## Column-Level Lineage

Below, each entity is traced field by field. <span class="new">Green</span> marks a column that is **created** in that layer (it has no upstream source).

### Customers

<div class="lineage-table-wrap" markdown>
<table class="lineage">
  <thead>
    <tr>
      <th class="raw">raw_customers.csv → seed</th>
      <th class="arrow"></th>
      <th class="bronze">bronze_customers</th>
      <th class="arrow"></th>
      <th class="silver">silver_customers</th>
    </tr>
  </thead>
  <tbody>
    <tr><td><code>customer_id</code> (string)</td><td class="arrow">→</td><td><code>customer_id</code></td><td class="arrow">→</td><td><code>customer_id</code> · <code>cast → integer</code></td></tr>
    <tr><td><code>first_name</code></td><td class="arrow">→</td><td><code>first_name</code></td><td class="arrow">→</td><td><code>first_name</code> · <code>trim()</code></td></tr>
    <tr><td><code>last_name</code></td><td class="arrow">→</td><td><code>last_name</code></td><td class="arrow">→</td><td><code>last_name</code> · <code>trim()</code></td></tr>
    <tr><td><code>email</code></td><td class="arrow">→</td><td><code>email</code></td><td class="arrow">→</td><td><code>email</code> · <code>lower(trim())</code></td></tr>
    <tr><td><code>signup_date</code></td><td class="arrow">→</td><td><code>signup_date</code></td><td class="arrow">→</td><td><code>signup_date</code> · <code>cast → date</code></td></tr>
    <tr><td><code>country</code></td><td class="arrow">→</td><td><code>country</code></td><td class="arrow">→</td><td><code>country</code> · <code>upper(trim())</code></td></tr>
    <tr><td></td><td class="arrow"></td><td class="new">+ _ingested_at, _ingested_by,<br>_source_file, _ingest_batch_id</td><td class="arrow">→</td><td class="new">transformed_at</td></tr>
  </tbody>
</table>
</div>

!!! note "Filter applied at silver"
    `where email is not null` — rows without an email are dropped, because customer marts key on it.

### Products

<div class="lineage-table-wrap" markdown>
<table class="lineage">
  <thead>
    <tr>
      <th class="raw">raw_products.csv → seed</th>
      <th class="arrow"></th>
      <th class="bronze">bronze_products</th>
      <th class="arrow"></th>
      <th class="silver">silver_products</th>
    </tr>
  </thead>
  <tbody>
    <tr><td><code>product_id</code> (string)</td><td class="arrow">→</td><td><code>product_id</code></td><td class="arrow">→</td><td><code>product_id</code> · <code>cast → integer</code></td></tr>
    <tr><td><code>product_name</code></td><td class="arrow">→</td><td><code>product_name</code></td><td class="arrow">→</td><td><code>product_name</code> · <code>trim()</code></td></tr>
    <tr><td><code>category</code></td><td class="arrow">→</td><td><code>category</code></td><td class="arrow">→</td><td><code>category</code> · <code>trim()</code></td></tr>
    <tr><td><code>unit_price</code></td><td class="arrow">→</td><td><code>unit_price</code></td><td class="arrow">→</td><td><code>unit_price</code> · <code>cast → decimal(12,2)</code></td></tr>
    <tr><td></td><td class="arrow"></td><td class="new">+ ingest metadata (×4)</td><td class="arrow">→</td><td class="new">transformed_at</td></tr>
  </tbody>
</table>
</div>

!!! note "Filter applied at silver"
    `where product_id is not null`.

### Orders

<div class="lineage-table-wrap" markdown>
<table class="lineage">
  <thead>
    <tr>
      <th class="raw">raw_orders.csv → seed</th>
      <th class="arrow"></th>
      <th class="bronze">bronze_orders</th>
      <th class="arrow"></th>
      <th class="silver">silver_orders</th>
    </tr>
  </thead>
  <tbody>
    <tr><td><code>order_id</code> (string)</td><td class="arrow">→</td><td><code>order_id</code></td><td class="arrow">→</td><td><code>order_id</code> · <code>cast → integer</code></td></tr>
    <tr><td><code>customer_id</code></td><td class="arrow">→</td><td><code>customer_id</code></td><td class="arrow">→</td><td><code>customer_id</code> · <code>cast → integer</code></td></tr>
    <tr><td><code>order_ts</code></td><td class="arrow">→</td><td><code>order_ts</code></td><td class="arrow">→</td><td><code>order_ts</code> · <code>cast → timestamp</code></td></tr>
    <tr><td><code>order_ts</code></td><td class="arrow">→</td><td>—</td><td class="arrow">→</td><td class="new">order_date · <code>cast(order_ts → date)</code></td></tr>
    <tr><td><code>status</code></td><td class="arrow">→</td><td><code>status</code></td><td class="arrow">→</td><td><code>status</code> · <code>lower(trim())</code></td></tr>
    <tr><td><code>payment_method</code></td><td class="arrow">→</td><td><code>payment_method</code></td><td class="arrow">→</td><td><code>payment_method</code> · <code>lower(trim())</code></td></tr>
    <tr><td></td><td class="arrow"></td><td class="new">+ ingest metadata (×4)</td><td class="arrow">→</td><td class="new">transformed_at</td></tr>
  </tbody>
</table>
</div>

!!! note "Filter + partitioning at silver"
    `where order_id is not null`. The table is **partitioned by `day(order_date)`** (PARQUET) so date-range queries prune files.

### Order items

<div class="lineage-table-wrap" markdown>
<table class="lineage">
  <thead>
    <tr>
      <th class="raw">raw_order_items.csv → seed</th>
      <th class="arrow"></th>
      <th class="bronze">bronze_order_items</th>
      <th class="arrow"></th>
      <th class="silver">silver_order_items</th>
    </tr>
  </thead>
  <tbody>
    <tr><td><code>order_item_id</code> (string)</td><td class="arrow">→</td><td><code>order_item_id</code></td><td class="arrow">→</td><td><code>order_item_id</code> · <code>cast → integer</code></td></tr>
    <tr><td><code>order_id</code></td><td class="arrow">→</td><td><code>order_id</code></td><td class="arrow">→</td><td><code>order_id</code> · <code>cast → integer</code></td></tr>
    <tr><td><code>product_id</code></td><td class="arrow">→</td><td><code>product_id</code></td><td class="arrow">→</td><td><code>product_id</code> · <code>cast → integer</code></td></tr>
    <tr><td><code>quantity</code></td><td class="arrow">→</td><td><code>quantity</code></td><td class="arrow">→</td><td><code>quantity</code> · <code>cast → integer</code></td></tr>
    <tr><td><code>discount_pct</code></td><td class="arrow">→</td><td><code>discount_pct</code></td><td class="arrow">→</td><td><code>discount_pct</code> · <code>cast → decimal(5,2)</code></td></tr>
    <tr><td></td><td class="arrow"></td><td class="new">+ ingest metadata (×4)</td><td class="arrow">→</td><td class="new">transformed_at</td></tr>
  </tbody>
</table>
</div>

!!! note "Filter applied at silver"
    `where quantity > 0`.

## Silver → Gold: where columns are computed

The gold layer is where four clean tables combine into business answers. These columns are **derived** — they don't exist upstream, they are calculated from joined silver columns.

### `gold_daily_sales` <span class="obj view">VIEW</span>

```mermaid
flowchart LR
  classDef silver fill:#f1f2f4,stroke:#6f7079,color:#161616;
  classDef gold   fill:#fcf4d6,stroke:#b28600,color:#161616;
  classDef col    fill:#ffffff,stroke:#c0c0c8,color:#161616;

  o["silver_orders"]:::silver
  i["silver_order_items"]:::silver
  p["silver_products"]:::silver

  od["order_date"]:::col
  cat["category"]:::col
  oc["order_count = count(distinct order_id)"]:::col
  us["units_sold = sum(quantity)"]:::col
  nr["net_revenue = sum(quantity × unit_price × (1 − discount_pct))"]:::col

  o -->|order_date| od
  p -->|category| cat
  o -->|order_id| oc
  i -->|quantity| us
  i -->|quantity, discount_pct| nr
  p -->|unit_price| nr

  od --> G["gold_daily_sales"]:::gold
  cat --> G
  oc --> G
  us --> G
  nr --> G
```

Joins: `silver_orders ⋈ silver_order_items` on `order_id`, then `⋈ silver_products` on `product_id`. Filter `status = 'completed'`, grouped by `order_date, category`.

### `gold_customer_360` <span class="obj view">VIEW</span>

```mermaid
flowchart LR
  classDef silver fill:#f1f2f4,stroke:#6f7079,color:#161616;
  classDef gold   fill:#fcf4d6,stroke:#b28600,color:#161616;
  classDef col    fill:#ffffff,stroke:#c0c0c8,color:#161616;

  c["silver_customers"]:::silver
  o["silver_orders"]:::silver
  i["silver_order_items"]:::silver
  p["silver_products"]:::silver

  prof["customer_id, first_name, last_name,<br>email, country, signup_date"]:::col
  cnt["completed / returned / pending /<br>cancelled_orders = count(distinct order_id)<br>filtered by status"]:::col
  ltv["lifetime_value = sum(quantity ×<br>unit_price × (1 − discount_pct))<br>where status = completed"]:::col
  ts["last_completed_order_ts,<br>last_activity_ts = max(order_ts)"]:::col

  c -->|profile columns| prof
  o -->|order_id, status| cnt
  o -->|status, order_id| ltv
  i -->|quantity, discount_pct| ltv
  p -->|unit_price| ltv
  o -->|order_ts, status| ts

  prof --> G["gold_customer_360"]:::gold
  cnt --> G
  ltv --> G
  ts --> G
```

Joins: `silver_customers` LEFT JOIN `silver_orders` on `customer_id`, LEFT JOIN `silver_order_items` on `order_id`, LEFT JOIN `silver_products` on `product_id`. Grouped per customer.

!!! tip "Trace one number end to end"
    `net_revenue` on the daily-sales dashboard =
    `raw_order_items.csv:quantity` × `raw_products.csv:unit_price` × (1 − `raw_order_items.csv:discount_pct`),
    summed for `completed` orders on a given `order_date` and `category`.
    Every factor is visible at every layer — that is the point of medallion.

## Two engines, same blueprint

dbt and Spark build the **same medallion shape** from the same CSVs, into separate schemas so you can compare them side by side.

| Layer | dbt path (Presto) | Spark path (PySpark) |
| --- | --- | --- |
| Raw | `dbt seed` → `lakehouse_demo_raw.*` <span class="obj table">TABLE</span> | CSVs read from `s3a://iceberg-bucket/spark_demo/raw` <span class="obj csv">CSV</span> |
| Bronze | `lakehouse_demo_bronze.bronze_*` <span class="obj table">TABLE</span> | `spark_demo_bronze.*` <span class="obj table">TABLE</span> |
| Silver | `lakehouse_demo_silver.silver_*` <span class="obj table">TABLE</span> | `spark_demo_silver.*` <span class="obj table">TABLE</span> |
| Gold | `lakehouse_demo_gold.gold_*` <span class="obj view">VIEW</span> | `spark_demo_gold.spark_gold_*` <span class="obj table">TABLE</span> |
{: .comparison-table }

Next: see the [dbt Demo Path](dbt-demo.md) or [Spark Demo Path](spark-demo.md) to build these layers yourself, then [compare them in SQL](sql-demo.md).
