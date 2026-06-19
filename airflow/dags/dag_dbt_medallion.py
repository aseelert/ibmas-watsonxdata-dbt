"""
DAG: dbt_medallion_hourly
=========================

Orchestrates the **dbt / Presto** medallion exactly as you run it by hand, but
with one Airflow task per table so the medallion build is visible end-to-end:

    bootstrap_schemas                 (scripts/bootstrap_watsonxdata.py)
        │                              create dbt_demo_{raw,bronze,silver,gold}
        ├── RAW    : dbt seed  --select raw_<x>      -> dbt_demo_raw
        ├── BRONZE : dbt run   --select bronze_<x>   -> dbt_demo_bronze
        ├── SILVER : dbt run   --select silver_<x>   -> dbt_demo_silver
        ├── GOLD   : dbt run   --select gold_<x>     -> dbt_demo_gold
        ├── dbt_test                                  (schema + data tests)
        └── query_gold                                (scripts/query_gold.py)

The task dependencies below are a 1:1 copy of the dbt ref() graph, so Airflow
runs models in true lineage order (and parallelises independent branches).

Why per-model BashOperators (and not one `dbt build`)?  This is a teaching
demo — each task = one table makes the medallion obvious in the Airflow graph.
The production-grade alternative is astronomer-cosmos, which renders the dbt
manifest into tasks automatically; see airflow/README.md.

Config: every connection value (host, API key, instance id, schemas, TLS cert)
comes from .env via the dbt profile (profiles/profiles.yml). Nothing is set
here. dbt's writable dirs (target/, logs/) are redirected via DBT_TARGET_PATH /
DBT_LOG_PATH (set in docker-compose) because the project is mounted read-only.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

try:  # TaskGroup location differs slightly across Airflow 3 point releases
    from airflow.sdk import TaskGroup
except ImportError:  # pragma: no cover
    from airflow.utils.task_group import TaskGroup

PROJECT_DIR = "/opt/airflow/project"

# The four medallion entities, in the order they flow through the layers.
ENTITIES = ["customers", "products", "orders", "order_items"]


def dbt_cmd(action: str, *, banner: str | None = None) -> str:
    """
    Build a dbt CLI invocation. DBT_PROFILES_DIR / DBT_TARGET_PATH / DBT_LOG_PATH
    are supplied by the environment (docker-compose) so the read-only project
    directory is never written to.

    A leading `echo "==> ..."` breadcrumb (defaulting to the dbt action) makes
    each task's progress obvious in the Airflow log.
    """
    step = banner or f"dbt {action}"
    return (
        f'echo "==> {step}" && '
        f"cd {PROJECT_DIR} && "
        f"dbt {action} --project-dir {PROJECT_DIR} --target dev --no-version-check"
    )


def script_cmd(relative_path: str, *, banner: str | None = None) -> str:
    """Run one of the repo's existing helper scripts (reused, not reimplemented)."""
    step = banner or f"run {relative_path}"
    return f'echo "==> {step}" && cd {PROJECT_DIR} && python {relative_path}'


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # Cap every task at 10 min (matches the sibling Spark DAG convention and the
    # user's "10 min per step" expectation); the DAG-level dagrun_timeout is the
    # whole-run backstop.
    "execution_timeout": timedelta(minutes=10),
}


@dag(
    dag_id="dbt_medallion_hourly",
    description="dbt/Presto medallion (raw→bronze→silver→gold) on watsonx.data, one task per table",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=30),
    max_active_runs=1,  # serialize hourly runs: no overlapping medallion builds
    tags=["dbt", "presto", "watsonx", "medallion"],
)
def dbt_medallion_hourly():
    # --- 0. Create the Iceberg schemas (reuses the existing bootstrap script) ---
    bootstrap = BashOperator(
        task_id="bootstrap_schemas",
        bash_command=script_cmd(
            "scripts/bootstrap_watsonxdata.py",
            banner="bootstrap: create dbt_demo_{raw,bronze,silver,gold} schemas",
        ),
    )

    # --- 1. RAW: load each seed CSV into dbt_demo_raw as its own task ---
    with TaskGroup(group_id="raw") as raw:
        seed = {
            # --full-refresh matches the standalone demo (docs/dbt-demo.md), so a
            # seed-schema change is picked up here the same way it is by hand.
            e: BashOperator(
                task_id=f"seed_raw_{e}",
                bash_command=dbt_cmd(
                    f"seed --select raw_{e} --full-refresh",
                    banner=f"RAW seed (full-refresh): raw_{e} -> dbt_demo_raw",
                ),
            )
            for e in ENTITIES
        }

    # --- 2. BRONZE: raw + ingestion metadata (one model per entity) ---
    with TaskGroup(group_id="bronze") as bronze:
        bronze_t = {
            e: BashOperator(
                task_id=f"bronze_{e}",
                bash_command=dbt_cmd(
                    f"run --select bronze_{e}",
                    banner=f"BRONZE: bronze_{e} -> dbt_demo_bronze",
                ),
            )
            for e in ENTITIES
        }

    # --- 3. SILVER: cleaned/typed dims+facts, then the enriched join ---
    with TaskGroup(group_id="silver") as silver:
        silver_t = {
            e: BashOperator(
                task_id=f"silver_{e}",
                bash_command=dbt_cmd(
                    f"run --select silver_{e}",
                    banner=f"SILVER: silver_{e} -> dbt_demo_silver",
                ),
            )
            for e in ENTITIES
        }
        silver_enriched = BashOperator(
            task_id="silver_sales_enriched",
            bash_command=dbt_cmd(
                "run --select silver_sales_enriched",
                banner="SILVER: silver_sales_enriched (enriched join) -> dbt_demo_silver",
            ),
        )
        # Standalone metrics time-spine (no upstream refs).
        time_spine = BashOperator(
            task_id="time_spine_daily",
            bash_command=dbt_cmd(
                "run --select time_spine_daily",
                banner="SILVER: time_spine_daily (metrics spine) -> dbt_demo_silver",
            ),
        )

    # --- 4. GOLD: business marts ---
    with TaskGroup(group_id="gold") as gold:
        gold_daily = BashOperator(
            task_id="gold_daily_sales",
            bash_command=dbt_cmd(
                "run --select gold_daily_sales",
                banner="GOLD: gold_daily_sales -> dbt_demo_gold",
            ),
        )
        gold_category = BashOperator(
            task_id="gold_category_performance",
            bash_command=dbt_cmd(
                "run --select gold_category_performance",
                banner="GOLD: gold_category_performance -> dbt_demo_gold",
            ),
        )
        gold_customer_360 = BashOperator(
            task_id="gold_customer_360",
            bash_command=dbt_cmd(
                "run --select gold_customer_360",
                banner="GOLD: gold_customer_360 -> dbt_demo_gold",
            ),
        )

    # --- 5. Tests + a real customer-facing query (reuses query_gold.py) ---
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_cmd("test", banner="TEST: dbt schema + data tests"),
    )
    # Bounded final preview: reuses scripts/query_gold.py (a fixed top-N query),
    # so it echoes the gold mart without any risk of hanging.
    query_gold = BashOperator(
        task_id="query_gold",
        bash_command=script_cmd(
            "scripts/query_gold.py",
            banner="PREVIEW: query gold mart (scripts/query_gold.py)",
        ),
    )

    # =====================================================================
    # Wire dependencies to mirror the dbt ref() graph exactly.
    # =====================================================================
    bootstrap >> raw
    bootstrap >> time_spine  # no refs; can build any time after schemas exist

    # raw_<x> -> bronze_<x> -> silver_<x>   (four independent branches)
    for e in ENTITIES:
        seed[e] >> bronze_t[e] >> silver_t[e]

    # all four silver entities -> enriched join
    for e in ENTITIES:
        silver_t[e] >> silver_enriched

    # enriched -> daily_sales -> category_performance
    silver_enriched >> gold_daily >> gold_category
    # customer_360 needs the customer dimension + the enriched fact
    [silver_t["customers"], silver_enriched] >> gold_customer_360

    # everything in gold must exist before tests, then the demo query
    [gold_daily, gold_category, gold_customer_360] >> dbt_test >> query_gold


dbt_medallion_hourly()
