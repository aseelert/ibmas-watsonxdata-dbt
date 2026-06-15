<section class="hero">
  <span class="eyebrow">IBM watsonx.data · medallion lakehouse demo</span>
  <h1>Learn watsonx.data, dbt, Spark, and lakehouse layers in one demo</h1>
  <p>
    This guide is for a technical beginner: someone who can run commands, but has never seen
    watsonx.data, dbt, Spark, or a medallion lakehouse before. You will load small CSV files,
    build clean lakehouse tables, run tests, submit a Spark job, and compare the results.
  </p>
  <div class="hero-actions">
    <a class="primary" href="setup/">Start the setup</a>
    <a href="lineage/">See the architecture</a>
    <a href="dbt-demo/">Run dbt path</a>
    <a href="spark-demo/">Run Spark path</a>
    <a href="sql-demo/">Copy SQL demo</a>
  </div>
</section>

<div class="brand-strip" markdown>
<span class="brand-label">Built on</span>
![IBM](assets/images/ibm.svg)
![watsonx.data](assets/images/watsonx.svg)
![dbt](assets/images/dbt.svg)
![Apache Spark](assets/images/spark.svg)
![Apache Iceberg](assets/images/iceberg.svg)
</div>

## New Here? Read This First

<p class="lede">If you have never heard of dbt, Spark, or a "lakehouse", don't worry. Here is the whole thing in everyday language before any commands appear. Think of a small online shop that wants to understand its own sales.</p>

!!! abstract "The story in one paragraph"
    A shop exports its records as four spreadsheet files (customers, products, orders, items). On their own, those files can't answer questions like *"how much did we sell on Tuesday?"* This demo takes those files and, step by step, turns them into clean, trustworthy tables you can ask questions of — and it does this **twice**, using two different tools (**dbt** and **Spark**) so you can see how each one works.

### The words you'll keep seeing

<div class="concept-grid">
  <div class="card">
    <h3>Lakehouse</h3>
    <p>A "data lake" stores tons of raw files cheaply. A "warehouse" lets you query tidy data with SQL. A <strong>lakehouse</strong> is both at once: cheap storage you can still query like a database.</p>
  </div>
  <div class="card">
    <h3>watsonx.data</h3>
    <p>IBM's lakehouse product — the <strong>building</strong> where this all happens. It holds the storage, the engines that run queries, and the catalog (the index of which tables exist).</p>
  </div>
  <div class="card">
    <h3>Medallion (bronze → silver → gold)</h3>
    <p>A way to organise data by quality, like refining metal. <strong>Raw</strong> ore → <strong>bronze</strong> (kept, labelled) → <strong>silver</strong> (cleaned) → <strong>gold</strong> (ready to use). Each step is more polished than the last.</p>
  </div>
  <div class="card">
    <h3>CSV file</h3>
    <p>A spreadsheet saved as plain text — just rows and commas. This is the raw starting data, the same kind of file Excel or Google Sheets can export.</p>
  </div>
  <div class="card">
    <h3>Table vs. View</h3>
    <p>A <strong>table</strong> is data actually stored on disk. A <strong>view</strong> is a saved question — it stores no data, it just re-runs its query and shows fresh results every time you look.</p>
  </div>
  <div class="card">
    <h3>dbt</h3>
    <p>A tool for <strong>cleaning data with SQL</strong>, the safe way. You write each transformation as a file; dbt runs them in the right order, tests them, and documents them — like recipes plus version control for your data.</p>
  </div>
  <div class="card">
    <h3>Spark</h3>
    <p>A powerful engine that splits big data work across <strong>many computers at once</strong>. Where dbt is one careful SQL cook, Spark is a whole kitchen brigade — built for large or heavy jobs.</p>
  </div>
  <div class="card">
    <h3>Iceberg</h3>
    <p>The open <strong>table format</strong> under the hood. It gives plain files database superpowers: safe updates, version history, and "time travel" to see how a table looked yesterday.</p>
  </div>
  <div class="card">
    <h3>Presto</h3>
    <p>The <strong>SQL engine</strong> that actually runs the queries on the dbt path. dbt writes the SQL; Presto executes it against watsonx.data.</p>
  </div>
</div>

!!! tip "Two tools, one goal — why both?"
    **dbt** and **Spark** are two different ways to do the same job here. dbt is the friendly choice when the work is *governed SQL analytics* (tests, documentation, lineage). Spark is the choice when the work is *big or heavy* (huge files, complex processing). This demo runs **both** on the same data so you can compare them honestly — neither overwrites the other.

## The Idea In One Minute

Imagine a shop exports four CSV files: customers, products, orders, and order lines. Those files are useful, but they are not yet a proper analytics system.

This demo turns those files into a lakehouse:

<div class="quick-grid">
  <div class="card">
    <div class="metric">1</div>
    <h3>Land the data</h3>
    <p>Keep the original CSV-shaped data visible so you can trace where records came from.</p>
  </div>
  <div class="card">
    <div class="metric">2</div>
    <h3>Add metadata</h3>
    <p>Bronze tables record ingest time, source file, and batch id.</p>
  </div>
  <div class="card">
    <div class="metric">3</div>
    <h3>Clean and type</h3>
    <p>Silver tables turn strings into dates, numbers, statuses, and reusable entities.</p>
  </div>
  <div class="card">
    <div class="metric">4</div>
    <h3>Publish marts</h3>
    <p>Gold tables answer business questions like daily sales and customer value.</p>
  </div>
</div>

## Raw Means Files First

In this demo, the **true raw layer** is the original CSV files:

```text
seeds/raw_customers.csv
seeds/raw_products.csv
seeds/raw_orders.csv
seeds/raw_order_items.csv
```

dbt and Spark use those raw files differently:

| Path | What happens to raw CSV files? | Why |
| --- | --- | --- |
| dbt | `dbt seed` loads the CSV files into `lakehouse_demo_raw` tables. | dbt transforms data with SQL, so the CSVs need to become queryable tables first. |
| Spark | Spark reads the uploaded CSV files directly from `s3a://iceberg-bucket/spark_demo/raw`. | Spark can read files from object storage directly, so it does not need a separate `spark_demo_raw` table schema. |

So the raw flow is:

```text
CSV files = true raw landing data

dbt path:
CSV files -> dbt seed raw tables -> bronze -> silver -> gold

Spark path:
CSV files in object storage -> bronze -> silver -> gold
```

## What Each Technology Does

<div class="concept-grid">
  <div class="card">
    <h3>watsonx.data</h3>
    <p>The lakehouse platform. It provides the catalog, engines, object storage access, and SQL surface.</p>
  </div>
  <div class="card">
    <h3>Iceberg</h3>
    <p>The open table format. It gives tables metadata, snapshots, partitions, and time travel.</p>
  </div>
  <div class="card">
    <h3>Presto</h3>
    <p>The SQL query engine used by dbt and the watsonx.data SQL editor in this demo.</p>
  </div>
  <div class="card">
    <h3>dbt</h3>
    <p>The SQL modeling tool. It builds, tests, and documents transformations.</p>
  </div>
  <div class="card">
    <h3>Spark</h3>
    <p>The distributed processing engine. It is strong for larger file and ETL jobs.</p>
  </div>
  <div class="card">
    <h3>MinIO</h3>
    <p>The S3-compatible object store where the Spark job reads its application and CSV files.</p>
  </div>
</div>

## How The Demo Flows

```mermaid
flowchart TB
  csv["CSV files: customers, products, orders, order items"]
  raw["Raw landing: source-shaped data"]
  bronze["Bronze: source data plus ingest metadata"]
  silver["Silver: clean typed reusable tables"]
  gold["Gold: analytics marts"]
  sql["SQL editor, BI, notebooks, customer demo"]

  csv --> raw --> bronze --> silver --> gold --> sql
```

The same source files are used in two different execution paths:

```mermaid
flowchart LR
  csv["Same raw CSV files"]
  dbtRaw["dbt seed raw tables"]
  sparkRaw["Spark reads CSVs from object storage"]
  dbt["dbt path: SQL through Presto"]
  spark["Spark path: PySpark job"]
  dbtGold["dbt gold: gold_daily_sales (table), gold_category_performance (view), gold_customer_360 (view)"]
  sparkGold["Spark gold tables: spark_gold_daily_sales, spark_gold_category_performance, spark_gold_customer_360"]
  compare["Compare results in watsonx.data SQL"]

  csv --> dbtRaw --> dbt --> dbtGold --> compare
  csv --> sparkRaw --> spark --> sparkGold --> compare
```

!!! note "Why two paths?"
    The demo keeps dbt and Spark separate so customers can compare them clearly. In real projects they often work together: Spark prepares big or complex data assets, then dbt governs the SQL models consumed by analytics teams.

## Medallion Layers

<div class="layer-grid">
  <div class="card raw">
    <span class="layer-tag">Raw</span>
    <h3>Raw</h3>
    <p>Original CSV payload, kept close to the source. Useful for traceability.</p>
  </div>
  <div class="card bronze">
    <span class="layer-tag">Bronze</span>
    <h3>Bronze</h3>
    <p>First managed Iceberg copy. Adds metadata like source file and batch id.</p>
  </div>
  <div class="card silver">
    <span class="layer-tag">Silver</span>
    <h3>Silver</h3>
    <p>Typed, cleaned, reusable business entities with validation tests.</p>
  </div>
  <div class="card gold">
    <span class="layer-tag">Gold</span>
    <h3>Gold</h3>
    <p>Business-facing marts for dashboards, SQL demos, and customer conversations.</p>
  </div>
</div>

For the full **column-by-column lineage** — how each CSV field becomes a typed silver column and feeds the gold marts — see [Architecture &amp; Lineage](lineage.md).

## Why Table Names Look Different

The dbt and Spark outputs are separated on purpose:

| dbt object | Spark object | Why |
| --- | --- | --- |
| `lakehouse_demo_gold.gold_daily_sales` | `spark_demo_gold.spark_gold_daily_sales` | Same business result, separate schema and Spark prefix. |
| `lakehouse_demo_gold.gold_category_performance` | `spark_demo_gold.spark_gold_category_performance` | Same category rollup, separate schema and Spark prefix. |
| `lakehouse_demo_gold.gold_customer_360` | `spark_demo_gold.spark_gold_customer_360` | Same customer mart, separate schema and Spark prefix. |

The dbt gold layer is now a **mix**: `gold_daily_sales` is a **table**, while `gold_category_performance` and `gold_customer_360` are **views**. Spark gold outputs are **physical Iceberg tables** because the Spark job writes dataframes into the catalog. That is normal.

## Strengths And Limits

| Tool | Strong at | Weaker at | In this demo |
| --- | --- | --- | --- |
| dbt | SQL transformations, tests, documentation, lineage, repeatable analytics models. | Heavy file processing, distributed non-SQL ETL, ML-style processing. | Builds `lakehouse_demo_*` schemas through Presto. |
| Spark | Distributed processing, large files, complex ETL, feature engineering, batch jobs near object storage. | Lightweight SQL governance, built-in model documentation, analyst-friendly review workflows. | Builds `spark_demo_*` schemas from uploaded CSV files. |
| watsonx.data | Shared Iceberg catalog, Presto SQL, Spark execution, object storage-backed lakehouse tables. | Transformation logic itself; dbt and Spark provide that logic. | Hosts the catalog, engines, and tables. |
{: .comparison-table }

## The Best Demo Order

<div class="path-list">
  <div class="path-step"><div><strong>Prepare the Python environment</strong><span>Create the virtual environment and install requirements.</span></div></div>
  <div class="path-step"><div><strong>Import the connection JSON</strong><span>Read watsonx.data host, instance id, and SSL certificate into local config.</span></div></div>
  <div class="path-step"><div><strong>Run the dbt path</strong><span>Create schemas, load seeds, build models, run tests, query gold.</span></div></div>
  <div class="path-step"><div><strong>Run the Spark path</strong><span>Upload Spark assets, submit the job, check status.</span></div></div>
  <div class="path-step"><div><strong>Compare outputs</strong><span>Use SQL to compare dbt and Spark gold results side by side.</span></div></div>
</div>

Start here: [Setup Order](setup.md).
