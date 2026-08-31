# Scripts and automation

The repository is a runnable demo, not just a slide deck. This page explains
the scripts in execution order and distinguishes safe validation commands from
operations that create or remove resources.

## The supported entry point

Use `bin/demo` for the normal workshop route. It keeps the demo commands short
and shields attendees from folder details until they need to inspect them.

| Command | What it does | Changes data or services? |
| --- | --- | --- |
| `bin/demo setup` | Prepares `.env`, certificates, endpoints, and discovery prerequisites | Yes; writes local configuration and may use `oc` |
| `bin/demo dbt build` | Runs the dbt reference transformation and tests | Yes; creates/updates Iceberg relations |
| `bin/demo metabase` | Starts the local Metabase compose stack | Yes; starts containers |
| `bin/demo spark` | Validates the Spark submission payload by default | No; dry-run unless configured otherwise |
| `bin/demo streaming` | Starts the local Kafka/Flink demonstration stack | Yes; starts containers and can seed events |
| `bin/demo airflow` | Starts the Airflow stack | Yes; starts containers |
| `bin/demo lineage` | Starts Marquez | Yes; starts containers |
| `bin/demo catalog` | Starts OpenMetadata and runs catalog ingestion | Yes; starts containers and writes metadata |
| `bin/demo validate` | Compares available Gold outputs | No; read-only queries |
| `bin/demo reset --dry-run` | Shows what a reset would remove | No |

## Environment and authentication

| Script | Purpose | When to use it |
| --- | --- | --- |
| `scripts/00a_prepare_watsonx_env.py` | Builds or refreshes the local `.env`, derives endpoints, manages certificate material, and can use `oc` for discovery | First setup and when cluster configuration changes |
| `scripts/00b_get_token.py` | Validates/refreshes the API-token path needed by services such as Spark | At the start of a working session or when a token expires |
| `scripts/01_bootstrap_watsonxdata.py` | Creates the demo schemas required by the lakehouse paths | Before the first full run on a clean environment |
| `scripts/02_dbt_env.sh` | Loads `.env`, selects the repo dbt profile, and calls dbt | Always use through `bin/demo dbt` or directly for advanced dbt commands |

The dbt profile is generated locally from
`01-dbt/profiles/profiles.example.yml`; it contains environment-variable
references rather than credentials. Keep `.env`, certificates, tokens, and
downloaded connection JSON files out of Git.

## Reference dbt path

```bash
bin/demo dbt debug
bin/demo dbt seed
bin/demo dbt build
bin/demo validate
```

`dbt build` is the recommended workshop command because it builds models and
runs the associated tests. The source fixtures are under `01-dbt/seeds/`; the
SQL models are organized under `01-dbt/models/bronze`, `silver`, and `gold`.

## Managed Spark path

| Script | Purpose | Default safety behavior |
| --- | --- | --- |
| `scripts/03a_upload_spark_assets.py` | Uploads the Spark application and source fixtures to the configured object-store location | Writes assets when run |
| `scripts/03b_submit_spark_application.py` | Builds and submits the watsonx.data Spark API request | Dry-run by default; set `WXD_SPARK_DRY_RUN=false` to submit |
| `scripts/03c_spark_application_status.py` | Polls or displays the submitted application status | Read-only |

The Python application is [load_medallion_demo.py](https://github.com/aseelert/ibmas-watsonxdata-dbt/blob/main/03-spark/spark/load_medallion_demo.py).
It is not a dbt model rendered in another form: it is a submitted distributed
application that reads assets from object storage and writes Iceberg tables.

## Ingestion, validation, and Iceberg demonstrations

| Script | Purpose |
| --- | --- |
| `scripts/04_ingest_with_cpdctl.py` | Demonstrates native watsonx.data ingestion through `cpdctl`; this lands data and does not replace downstream transformation |
| `scripts/05_query_gold.py` | Runs readable Gold-level verification queries |
| `scripts/06_demo_time_travel.py` | Demonstrates Iceberg snapshot/time-travel behavior where the target engine supports the query |
| `scripts/reconcile_gold.py` | Compares the Gold results across available dbt, Spark, and streaming paths |

## Governance and catalog automation

| Script | Purpose |
| --- | --- |
| `scripts/06b_provision_ikc_governance.py` | Provisions the sample governance assets when the target IBM capability is available |
| `scripts/07a_prepare_openmetadata_dbt_artifacts.py` | Prepares dbt artifacts for OpenMetadata ingestion |
| `scripts/07b_generate_lineage_docs.sh` | Generates lineage-related documentation artifacts |
| `scripts/07c_upload_dbt_artifacts.py` | Uploads prepared dbt artifacts to the configured metadata flow |
| `scripts/07d_apply_openmetadata_governance.py` | Applies the sample OpenMetadata governance enrichment |
| `scripts/10_configure_ikc_reporting.sh` | Configures optional IKC reporting integration |
| `scripts/10b_provision_pg_reporting.sh` and `10c_pg_reporting.py` | Provision and load the optional PostgreSQL reporting example |

## Reset and cleanup

Cleanup is intentionally explicit because it can remove containers, volumes,
schemas, object-store files, or data created by this demo.

```bash
bin/demo reset --dry-run
scripts/11_reset_demo.sh --all --dry-run
```

| Script | Scope |
| --- | --- |
| `scripts/08_cleanup_watsonxdata.py` | Lakehouse schemas and objects selected by its options |
| `scripts/09_cleanup_minio.py` | Demo object-store assets selected by its options |
| `scripts/11_reset_demo.sh` | Coordinated Docker, warehouse, and/or object-store reset; use `--dry-run` first |

The cleanup scripts are part of the demo story: a workshop must be repeatable
from a known starting state, but a presenter should never run a broad reset
against a shared environment without checking the resolved targets.
