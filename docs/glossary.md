# Beginner Glossary

Short definitions for the words used in this demo.

## Lakehouse

A lakehouse is a data platform that combines:

- cheap file/object storage like a data lake
- table structure and SQL access like a data warehouse

In this demo, watsonx.data is the lakehouse platform.

## Catalog

A catalog is the place where table names and metadata live.

When you query:

```sql
select *
from iceberg_data.lakehouse_demo_gold.gold_daily_sales;
```

`iceberg_data` is the catalog.

## Schema

A schema is a namespace inside a catalog. It groups tables.

Example:

```text
iceberg_data.lakehouse_demo_silver
```

## Table

A table is the object you query with SQL. In this demo most physical tables are Iceberg tables.

## View

A view is a saved SQL query. It looks like a table when you query it, but it does not store its own copy of the data.

dbt gold is configured as views by default:

```text
gold_daily_sales
gold_customer_360
```

## Iceberg

Apache Iceberg is an open table format. It stores table metadata so engines like Presto and Spark can safely work with the same lakehouse tables.

Iceberg supports useful features such as:

- snapshots
- partition metadata
- time travel
- schema evolution

## Presto

Presto is a SQL engine. It runs SQL queries against lakehouse tables.

In this demo:

- dbt sends SQL to Presto
- watsonx.data SQL editor uses Presto
- Python helper scripts query Presto

## dbt

dbt is a tool for building SQL transformations as a project.

It gives you:

- SQL model files
- repeatable builds
- tests
- model dependencies
- documentation and lineage

It is strong when the transformation logic is SQL.

## Spark

Spark is a distributed processing engine. It can use many workers to process files and large datasets.

It is strong when the work is:

- large
- file-heavy
- complex ETL
- machine-learning or feature engineering oriented

## MinIO

MinIO is S3-compatible object storage. In this demo Spark reads the PySpark application and CSV files from MinIO.

## Medallion Architecture

Medallion architecture is a common way to organize data quality layers:

```text
raw -> bronze -> silver -> gold
```

Each layer makes the data more useful:

| Layer | Meaning |
| --- | --- |
| Raw | Source-shaped input data. |
| Bronze | Managed copy with ingest metadata. |
| Silver | Clean reusable business data. |
| Gold | Business-ready marts. |

## Raw Files Versus Raw Tables

The raw files are the original input files.

In this demo:

```text
seeds/raw_*.csv
```

dbt also creates raw tables:

```text
iceberg_data.lakehouse_demo_raw.raw_*
```

Those tables exist because dbt works through SQL. Spark does not need `spark_demo_raw` tables because Spark reads CSV files directly from object storage.

## Customer 360

Customer 360 means a customer-level table that combines many signals into one row per customer.

In this demo it includes:

- completed orders
- returned orders
- pending orders
- cancelled orders
- lifetime value
- last completed order timestamp
- last activity timestamp

## Time Travel

Time travel means querying an older version of an Iceberg table.

Example:

```sql
select *
from iceberg_data.lakehouse_demo_silver.silver_orders
for version as of <snapshot_id>;
```

This is useful for demos because it shows that the table format stores history, not just the current rows.
