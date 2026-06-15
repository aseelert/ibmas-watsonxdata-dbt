<section class="hero">
  <span class="eyebrow">IBM watsonx.data · Ingestion Workshop</span>
  <h1>Three ways to get data into a lakehouse — dbt, Spark, and cpdctl</h1>
  <p>
    A hands-on workshop for anyone new to watsonx.data. You load the same four CSV files three
    different ways, then compare the results. No prior experience with dbt, Spark, or IBM tools
    required.
  </p>
  <div class="hero-actions">
    <a class="primary" href="setup/">Start Preparation</a>
    <a href="lineage/">See Architecture</a>
    <a href="dbt-demo/">Run dbt</a>
    <a href="spark-demo/">Run Spark</a>
    <a href="ingestion/">Try cpdctl</a>
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

## What you will learn

!!! info "Workshop learning goals"
    By the end of this workshop you will be able to:

    - Explain what watsonx.data is and how its parts (catalog, engines, object storage) fit together
    - Describe three real ways to load and transform data in a lakehouse — and choose the right one
    - Run a governed SQL pipeline with dbt and Presto, including tests and lineage
    - Run a distributed Python ETL job with Spark and verify its output with SQL

## What we are building

A small online shop has four CSV files: customers, products, orders, and order items — 1,134 rows
in total. We load those files into watsonx.data three different ways. Each path produces the same
Bronze, Silver, and Gold tables, but writes to its own schema so the outputs never collide. At the
end, you query all three results side by side with a single SQL statement.

```mermaid
flowchart TB
  csv["4 CSV files\ncustomers · products · orders · order_items"]

  csv --> dbt_path["Path A — dbt\nSQL + Presto"]
  csv --> spark_path["Path B — Spark\nPySpark ETL"]
  csv --> cpdctl_path["Path C — cpdctl\nIBM CLI"]

  dbt_path --> bronze["Bronze\nlakehouse_demo_bronze"]
  spark_path --> spark_bronze["Bronze\nspark_demo_bronze"]
  cpdctl_path --> ingest_bronze["Bronze\nlakehouse_demo_ingest"]

  bronze --> silver["Silver\nlakehouse_demo_silver"]
  spark_bronze --> spark_silver["Silver\nspark_demo_silver"]
  ingest_bronze --> ingest_silver["Silver\n(query with SQL)"]

  silver --> gold["Gold marts\nlakehouse_demo_gold"]
  spark_silver --> spark_gold["Gold marts\nspark_demo_gold"]

  gold --> compare["SQL comparison\nwatsonx.data query editor"]
  spark_gold --> compare
  ingest_silver --> compare
```

## The three ingestion paths

Every path reads the same source files and produces Iceberg tables in Parquet format. The
difference is which tool drives the work and what governance features come with it.

| Path | Tool | Language | Best for | Shows in UI ingestion history? |
|------|------|----------|----------|-------------------------------|
| A — dbt | dbt + Presto | SQL | Governed analytics, built-in tests, column lineage | No |
| B — Spark | PySpark on watsonx.data Spark engine | Python | Large files, distributed ETL, complex transformations | No (appears under Spark Applications) |
| C — cpdctl | IBM CLI (`cpdctl`) | Shell | Native UI-tracked ingestion, no code required | Yes |

All three paths write Iceberg tables stored as Parquet files in MinIO object storage. They write to
different schemas (`lakehouse_demo_*`, `spark_demo_*`, `lakehouse_demo_ingest`) so you can compare
them without one path overwriting another.

!!! note "Why three paths instead of one?"
    Real teams choose different tools for different reasons — governance requirements, file size,
    skill set, or whether they want ingestion history in the UI. Running all three on the same data
    lets you see the trade-offs directly, not just read about them.

## The medallion pattern

The medallion pattern is a way to organize data by quality level, moving from raw files to
production-ready analytics tables in three named layers. Each layer adds something the previous
one lacked — metadata, type safety, or business logic. All three ingestion paths in this workshop
follow the same Bronze → Silver → Gold progression.

```mermaid
flowchart LR
  raw["Raw CSV files\n(source files)"]
  bronze["Bronze\ncopy + ingest metadata"]
  silver["Silver\nclean + typed"]
  gold["Gold\nbusiness marts"]

  raw --> bronze --> silver --> gold
```

| Layer | Plain-English meaning | What is added |
|-------|-----------------------|---------------|
| Raw | The original CSV files exactly as exported from the shop system | Nothing — this is the starting point |
| Bronze | A first managed copy in the lakehouse | Ingest timestamp, source file name, batch ID |
| Silver | Clean, typed, validated business entities | Proper dates, numeric types, status enums, deduplication |
| Gold | Pre-aggregated answers to business questions | Daily sales totals, category rankings, customer lifetime value |

!!! tip "Table vs. view in the gold layer"
    In the dbt path, `gold_daily_sales` is a physical **table** (data stored on disk). The other
    two gold objects — `gold_category_performance` and `gold_customer_360` — are **views** (saved
    queries that re-run on demand). The Spark path writes all gold outputs as physical Iceberg
    tables. Both approaches are valid; the dbt mix shows how you choose per use case.

## Workshop flow

Work through the pages in this order. Each step builds on the last.

1. **Prepare** ([setup.md](setup.md)) — ~15 min
   Install Python dependencies, configure the watsonx.data connection, and verify access to the
   Presto endpoint at `ibm-lh-lakehouse-presto651-presto-svc.apps.watson.ibmas-zocp-techcluster.org:443`.

2. **Understand the architecture** ([lineage.md](lineage.md)) — read only
   See the full column-by-column lineage and understand how schemas relate before you run anything.

3. **Run Path A: dbt** ([dbt-demo.md](dbt-demo.md)) — ~20 min
   Create schemas, load seeds into `lakehouse_demo_raw`, build Bronze/Silver/Gold models, run dbt
   tests, and query the gold layer.

4. **Run Path B: Spark** ([spark-demo.md](spark-demo.md)) — ~15 min
   Upload the PySpark job and CSV files to MinIO, submit the job to the watsonx.data Spark engine,
   and verify the `spark_demo_*` schemas.

5. **Run Path C: cpdctl** ([ingestion.md](ingestion.md)) — ~10 min
   Use the IBM CLI to ingest the CSV files via the native watsonx.data ingestion API, then check
   the ingestion history in the UI.

6. **Compare results with SQL** ([sql-demo.md](sql-demo.md))
   Run side-by-side queries across all three gold schemas to confirm they produce the same numbers.

7. **Explore lineage in OpenMetadata** ([openmetadata.md](openmetadata.md))
   Open OpenMetadata at `http://localhost:8585` to visualize the dbt lineage graph — from seed
   tables through to gold marts.

!!! warning "Complete the setup page before running any path"
    Paths A, B, and C all require a working connection profile and Python environment. Skipping
    the setup page is the most common reason commands fail.

## The words you will keep seeing

<div class="concept-grid">
  <div class="card">
    <h3>watsonx.data</h3>
    <p>IBM's lakehouse platform — the environment where this workshop runs. It provides the catalog (index of all tables), query engines, and object storage access in one place.</p>
  </div>
  <div class="card">
    <h3>Iceberg</h3>
    <p>The open table format used for every table in this workshop. It gives plain Parquet files database features: safe updates, snapshot history, and partition management.</p>
  </div>
  <div class="card">
    <h3>Presto</h3>
    <p>The SQL query engine built into watsonx.data. dbt sends SQL to Presto, Presto executes it against the Iceberg catalog, and results come back as a result set.</p>
  </div>
  <div class="card">
    <h3>dbt</h3>
    <p>A tool that turns SQL SELECT statements into managed data pipelines. You write each transformation as a <code>.sql</code> file; dbt runs them in the right order, tests them, and tracks lineage.</p>
  </div>
  <div class="card">
    <h3>Spark</h3>
    <p>A distributed processing engine that runs Python (PySpark) jobs across multiple workers. Used in Path B to read CSV files from MinIO and write Iceberg tables.</p>
  </div>
  <div class="card">
    <h3>cpdctl</h3>
    <p>The IBM Cloud Pak for Data CLI. In Path C it calls the watsonx.data ingestion API directly, producing an ingestion job that appears in the platform UI history.</p>
  </div>
</div>
