# OpenLineage: The Open Standard for Data Lineage

!!! info "What is OpenLineage? (no jargon)"
    OpenLineage is a **common language for recording where data came from and what transformed it**. Today every tool — dbt, Spark, Airflow, a BI dashboard — has its own private idea of "lineage." OpenLineage agrees on one shared vocabulary so every tool can contribute to *one* lineage picture: this job read those tables, ran this transformation, and produced that table. Think of it as a shared notebook every pipeline tool writes into, so you never have to stitch the story together by hand.

## What OpenLineage is (the technical version)

OpenLineage is an **open specification** plus a set of integrations. As a pipeline runs, an OpenLineage *integration* emits **events** describing each **run** of a **job** and the input/output **datasets** it touched (with optional schema and column-level facets). Those events are sent to a **collector** — commonly [Marquez](https://marquezproject.io/) (the reference implementation) or [OpenMetadata](openmetadata.md), both of which can consume OpenLineage events. Ready-made integrations exist for **Spark** (a listener jar on the Spark session), **Airflow** (a built-in listener that emits an event per task), and **dbt** (`dbt-ol`, which wraps a dbt run). The payoff is consistent, automatically-collected lineage across tools that were never designed to talk to each other.

OpenLineage is **not** a catalog by itself. It is the event format. You still need something to receive, store, search, and display those events. That receiver might be Marquez, OpenMetadata, or an enterprise governance platform that supports a compatible ingestion path.

| Concept | Meaning |
| --- | --- |
| Job | The thing that runs, for example a dbt model build, Spark application, or Airflow task. |
| Run | One execution of that job, with timestamps and status. |
| Dataset | An input or output table/file/topic touched by the run. |
| Facet | Extra structured metadata: schema, column lineage, data quality, SQL text, parent run, processing engine, and more. |
| Collector | The service that receives OpenLineage events and persists or forwards them. |

---

## How lineage works in THIS demo today

!!! abstract "Honest status: OpenLineage is not wired in this repo"
    This repo does **not** currently emit OpenLineage events — there is no OpenLineage listener on Spark, no `dbt-ol` wrapper, and no Marquez collector. OpenLineage is included here as the **concept and standard** you would reach for to unify lineage across tools. Lineage in this demo is captured a different (and perfectly valid) way: from **dbt's build artifacts**, ingested into OpenMetadata.

What actually produces the lineage graph you see in the [OpenMetadata page](openmetadata.md):

1. dbt runs against Presto and `dbt docs generate` writes three JSON artifacts — `manifest.json` (the full model/`ref()` graph), `catalog.json` (column names + types), and `run_results.json` (which tests passed).
2. Those artifacts are prepared by the repo's helper scripts (`scripts/prepare_openmetadata_dbt_artifacts.py` for a full build, or `scripts/generate_lineage_docs.sh` to refresh lineage only) and ingested by `openmetadata/ingestion/run-ingestion.sh`.
3. OpenMetadata first tries to discover the real tables through live Presto metadata ingestion. If that fails, it seeds the same table entities from `catalog.json`.
4. OpenMetadata reads `manifest.json` to draw the **model and column lineage** — `raw CSV → bronze → silver_sales_enriched → gold_daily_sales` — and the governance pass applies glossary terms and auto-classification tags. The full column-by-column trace is documented on the [Architecture & Lineage page](lineage.md).

```mermaid
flowchart LR
  classDef have fill:#defbe6,stroke:#198038,color:#161616;
  classDef would fill:#f6f2ff,stroke:#6929c4,color:#161616,stroke-dasharray:4 3;

  dbt["dbt run + dbt docs generate"]:::have
  art["manifest.json / catalog.json /\nrun_results.json"]:::have
  om["OpenMetadata\nlineage + tests UI"]:::have
  dbt --> art --> om

  spark["Spark / Airflow\nOpenLineage listener"]:::would
  col["OpenLineage collector\n(Marquez / OpenMetadata)"]:::would
  spark -. "events (not wired today)" .-> col
  col -. "unified lineage" .-> om
```

The solid path is what runs in this repo today. The dashed path is how OpenLineage **would** plug in.

## OpenMetadata vs OpenLineage

OpenMetadata and OpenLineage complement each other:

| Tool | What it is | What it gives you |
| --- | --- | --- |
| OpenMetadata | Catalog, governance model, and UI. | Searchable tables, columns, tags, glossary terms, owners, tests, and lineage views. |
| OpenLineage | Open runtime lineage event standard. | A common way for dbt, Spark, Airflow, and other tools to report what they read and wrote while jobs execute. |

In this repo, OpenMetadata is the working UI and catalog. OpenLineage is the future-facing standard you would use when lineage should be emitted by every runtime system, not only reconstructed from dbt artifacts.

### How OpenLineage would be added

To capture lineage as live OpenLineage events instead of (or alongside) dbt artifacts, you would:

- **Spark:** add the OpenLineage Spark listener jar to `spark/load_medallion_demo.py`'s session config (`spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener`) pointing at a collector URL — every read/write becomes a lineage event.
- **Airflow:** enable the OpenLineage provider so each task in `dbt_medallion_hourly` / `spark_medallion_hourly` (see the [Airflow page](airflow.md)) emits run events automatically.
- **dbt:** wrap runs with `dbt-ol run` to emit events from the dbt model graph.
- **Collector:** stand up Marquez (or point the events at OpenMetadata) to view the merged graph.

That upgrade would give you lineage across **all three engines at once**, including Spark — which dbt artifacts alone cannot capture, because dbt only knows about the dbt models.

## Enterprise Pattern: IBM Governance and Manta

OpenLineage is especially useful in larger IBM governance landscapes because it gives every execution engine a common lineage envelope before metadata is sent to a catalog or lineage product. For example, a production architecture could collect events from this demo's dbt, Spark, and Airflow paths and then feed the normalized metadata into IBM Knowledge Catalog / watsonx.data intelligence or a Manta lineage deployment, using whatever connector, API, export/import, or bridge is supported by that environment.

This is deliberately described as an **integration pattern**, not as something this repo already performs:

```mermaid
flowchart LR
  dbt["dbt runs"] --> ol["OpenLineage events"]
  spark["Spark jobs"] --> ol
  airflow["Airflow tasks"] --> ol
  ol --> collector["Collector / bridge"]
  collector --> om["OpenMetadata\nlocal catalog"]
  collector -. "enterprise ingestion path" .-> ikc["IBM Knowledge Catalog /\nwatsonx.data intelligence"]
  collector -. "enterprise lineage path" .-> manta["Manta lineage"]
```

The local workshop uses OpenMetadata because it is fast to run in Docker and easy to inspect. In a client production estate, OpenLineage can become the shared event format that helps dbt, Spark, Airflow, IBM catalog services, and lineage tools speak the same lineage language.

!!! note "📸 Screenshot: the lineage graph"
    Capture the OpenMetadata **Lineage** tab for `gold_daily_sales` showing the `raw → bronze → silver → gold` chain (this is the lineage this repo produces today), then save it to `docs/assets/images/screenshots/lineage-graph.png` and replace this note with the image.

---

See the [OpenMetadata page](openmetadata.md) for the working lineage UI and the [Architecture & Lineage page](lineage.md) for the full column-by-column trace.
