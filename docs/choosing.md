# When to Use Which — and How the Paths Combine

!!! abstract "The one idea to take away"
    **dbt and Spark are two interchangeable, self-contained pipelines** — each *ingests and
    transforms* the CSVs all the way through Bronze → Silver → Gold. **cpdctl is an ingestion-only
    loader** — like `dbt seed`, it lands the raw CSVs in `spark_demo_cpdctl_raw` and *stops at raw*.
    To turn cpdctl data into a medallion, you run **dbt or Spark as a post-action** over the ingest
    schema.

    > **cpdctl (ingest) + dbt or Spark (transform) = one full pipeline.**

---

## Two engines, one loader

| | dbt | Spark | cpdctl |
|---|---|---|---|
| **Kind** | Full pipeline | Full pipeline | Ingest loader only |
| **Does it transform?** | Yes — raw → bronze → silver → gold | Yes — bronze → silver → gold | **No** — lands raw and stops |
| **Language** | SQL | Python (PySpark) | CLI (no code) |
| **Builds a medallion alone?** | Yes | Yes | No — needs dbt or Spark afterward |
| **Output schema** | `dbt_demo_raw/bronze/silver/gold` | `spark_demo_bronze/silver/gold` | `spark_demo_cpdctl_raw` (raw) |
| **UI ingestion history?** | No | No | **Yes** |
| **Analogous to** | — | — | `dbt seed` / Spark's raw CSV read |

dbt and Spark are **peers** — pick whichever engine fits the team and workload; both produce the
same medallion shape and the [SQL comparison](sql-demo.md) proves their gold layers match exactly.
cpdctl is **not** a third peer engine: it is a fast, UI-tracked way to get raw data *in*, which you
then hand to dbt or Spark.

---

## dbt vs Spark — at a glance

dbt and Spark are interchangeable peers in this demo, but they make different trade-offs. The
table below lays them side by side so you can match the engine to the team and the workload.

| Dimension | dbt (`models/*.sql`) | Spark (`spark/load_medallion_demo.py`) |
|---|---|---|
| Language | SQL + Jinja | Python / PySpark (Scala/Java possible) |
| Where compute runs | Pushed down to the Presto engine in watsonx.data — dbt only compiles + orchestrates, no data on your laptop | Distributed on the watsonx.data Spark engine — executors process partitions in parallel |
| Transformation style | Set-based, declarative — one `SELECT` per model | Imperative dataframe ETL — read → withColumn → join → groupBy → writeTo |
| Materialization | table / view / incremental via `{{ config }}` | Explicit `createOrReplace()`, `partitionedBy(...)` |
| Best for | Analytics modeling, governed SQL marts, dimensional models | Heavy ETL, complex logic, huge joins, semi-structured parsing, ML feature prep |
| Testing | First-class built-in (`not_null`/`unique`/`relationships`/custom in YAML), `dbt test` | DIY (PySpark asserts, Great Expectations, pytest) — no native runner |
| Docs | Auto-generated searchable site (`dbt docs`) | Manual docstrings / READMEs |
| Lineage | Automatic column-level from `ref()`/`source()`; flows into OpenMetadata | Inferred from code or a runtime OpenLineage listener; more setup |
| Governance | Strong: SQL code review + declared tests + lineage + docs-as-code | Lives in the surrounding platform/CI; general-purpose Python is harder to police |
| Learning curve | Low for SQL/analytics people | Higher — Spark APIs, lazy evaluation, partitioning, shuffles, tuning |
| Performance — small data | Excellent; no cluster spin-up | Overkill — JVM/executor startup dominates |
| Performance — big data | Scales with Presto | Built for very large multi-TB workloads |
| Streaming | Batch only | Structured Streaming / micro-batch |
| ML feature engineering | SQL-expressible only | Full Python ecosystem (pandas UDFs, MLlib) |
| When to pick | SQL-first team; logic fits a `SELECT`; want tests+docs+lineage for free | Large data, or Python/ML/streaming/custom parsing |

**When to use which.** Start with **dbt** for SQL analytics engineering on watsonx.data: the compute
runs on the same Presto engine you already query, and you get tests, docs, and column-level lineage
almost for free. Reach for **Spark** when you outgrow SQL and Presto — very large data,
billions-of-rows joins, streaming, ML feature engineering, or messy semi-structured parsing that SQL
cannot express cleanly. They are interchangeable peers here, and a common real-world pattern uses
**both**: Spark for the heavy raw → Silver lift, dbt for the governed Silver → Gold modeling. And
remember **cpdctl is ingestion-only** — it lands the raw files and stops; you still run dbt or Spark
on top to build the medallion.

---

## Choose by the job

Find the row that matches your situation, then pick the engine in the last column.

| Your situation | Pick this |
|----------------|-----------|
| "My logic is all `SELECT` statements and I want tests + lineage" | **dbt** |
| "I'm a SQL-first analytics team on watsonx.data" | **dbt** |
| "The data is huge, or I need Python / ML / streaming / messy parsing" | **Spark** |
| "I want distributed compute across many workers" | **Spark** |
| "I just need raw data loaded fast, with no code, tracked in the UI" | **cpdctl** (then transform with dbt or Spark) |
| "Heavy raw→Silver lift, then governed Silver→Gold modeling" | **Spark for the lift, dbt for the modeling** |

=== "Use dbt"

    - Your team treats transformations as **governed SQL** — code review, tests, lineage,
      documentation-as-code.
    - You want column-level lineage in OpenMetadata.
    - The logic fits in SQL `SELECT` statements.

=== "Use Spark"

    - The data is **large** or the logic needs **Python** (ML feature prep, custom parsing,
      billions-of-rows joins).
    - You want distributed compute on the watsonx.data Spark engine.

=== "Use cpdctl"

    - You need the load to appear in the **watsonx.data console → Data manager → Ingestion** audit
      history.
    - You want a fast, **no-code** raw load from a terminal or the console GUI.
    - Then you still run **dbt or Spark** over `spark_demo_cpdctl_raw` to build bronze/silver/gold.

---

## Seeds vs Sources — seed the lookups, source the raw

!!! tip "Seed the lookups, source the raw"
    dbt **seeds** are for small (low-thousands), static, version-controlled lookup/reference data —
    status → label maps, country/region codes, currency tables. They are **not** meant for real
    raw or production data. Real raw data should **land** in a raw zone by an ingestion process and
    be read by dbt as a `{{ source() }}` declared in a `sources.yml` (so you can also run
    `dbt source freshness` against it).

    Here is how **this repo** currently works (a teaching point, not a criticism): today the dbt
    path loads `seeds/raw_*.csv` via `dbt seed` and the bronze models read them with
    `{{ ref('raw_*') }}` — i.e. seeds used as a stand-in raw zone. The Spark path already reads the
    same CSVs from the object-storage raw zone (`s3a://.../spark_demo/raw`), and the cpdctl path
    lands them in `spark_demo_cpdctl_raw`.

    One rule of thumb:

    > *If a human curates it in git → seed it. If a pipeline lands it in the raw zone → source it.*

---

## Separate vs together

**Separate** — dbt and Spark each run end to end on their own:

```text
dbt:   seeds/ CSV → dbt_demo_raw → bronze → silver → gold
Spark: object-store CSV → spark_demo_bronze → silver → gold
```

**Together** — cpdctl ingests, then dbt or Spark transforms:

```text
cpdctl: CSV → spark_demo_cpdctl_raw (raw)
            → [post-action] dbt or Spark transform → bronze → silver → gold
```

```mermaid
flowchart LR
    CSV["CSV files"]
    CSV --> dbt["dbt (full pipeline)"]
    CSV --> spark["Spark (full pipeline)"]
    CSV --> cpdctl["cpdctl ingest\nspark_demo_cpdctl_raw (raw)"]
    cpdctl -. "transform with dbt or Spark\n(post-action)" .-> dbt
    cpdctl -. "transform with dbt or Spark\n(post-action)" .-> spark
    dbt --> gold["dbt gold"]
    spark --> sgold["Spark gold"]
    gold --> compare["compare gold-to-gold"]
    sgold --> compare
```

The worked post-action examples (a dbt model and a Spark read over `spark_demo_cpdctl_raw`) are in
[What cpdctl does NOT do — and how to finish the job](ingestion.md#what-cpdctl-does-not-do-and-how-to-finish-the-job).

---

## Next step

- New here? Start with [Architecture & Data Flow](lineage.md).
- Ready to build? Run [Path A — dbt](dbt-demo.md) or [Path B — Spark](spark-demo.md).
- Want UI-tracked ingestion? Run [cpdctl](ingestion.md), then transform with dbt or Spark.
- Built both engines? [Compare the dbt and Spark gold layers](sql-demo.md).
