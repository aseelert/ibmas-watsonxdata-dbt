# -----------------------------------------------------------------------------
#  dag_spark_medallion.py — Airflow DAG: submit one Spark job, verify each layer
#
#  Location  : airflow/dags/dag_spark_medallion.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Airflow DAG that submits one watsonx.data Spark job, then verifies each layer.

DAG: spark_medallion_hourly
===========================

Orchestrates the **watsonx.data Spark** medallion — the same steps you run by
hand (upload assets → authenticate → submit → wait → check results):

    [prepare]  upload_assets        (scripts/upload_spark_assets.py)   * optional
    [auth]     get_token            mint a CPD bearer token from WXD_API_KEY
    [build]    submit_spark_app     POST load_medallion_demo.py to the Spark engine
               wait_for_spark       poll the app until state == finished
    [verify]   validate_bronze / validate_silver / validate_gold
                                    one Presto task per layer, counting the
                                    spark_demo_{bronze,silver,gold} tables
    [query]    query_gold_spark     a real business query on the gold mart

Design note — why the build is ONE task, not one-per-table:
  Spark builds the whole medallion inside a single distributed application
  (load_medallion_demo.py writes bronze→silver→gold in one job). That is the
  Spark paradigm; splitting it would mean editing that app, which we must not.
  So we keep the submit as one task and instead make each *layer* its own
  verification task — giving the same raw→bronze→silver→gold shape as the dbt
  DAG, honestly reflecting how Spark actually runs.

Config: all values come from .env (loaded into the container). Auth/TLS logic
is shared with the dbt DAG via airflow/dags/common/wxd.py — never duplicated.

WHEN to run
  Auto-scheduled @hourly (catchup=False, max_active_runs=1). Trigger manually
  from the UI for a demo. The Spark app + raw CSVs must already exist in the
  object store — either set the `upload_assets` param to True (uploads via
  scripts/upload_spark_assets.py, needs MinIO reachable from the container) or,
  more usually, run that upload once on the host beforehand.

PARAMS
  * upload_assets (bool, default False) — push the Spark app + CSVs to MinIO
    first.  * dry_run (bool, default False) — build and print the redacted Spark
    payload without submitting (the wait sensor short-circuits to success).

ENV VARS
  Spark sizing/targets: WXD_SPARK_APPLICATION, WXD_SPARK_INPUT_BASE,
  WXD_SPARK_CATALOG, WXD_SPARK_SCHEMA (+ _BRONZE/_SILVER/_GOLD_SCHEMA),
  WXD_SPARK_EXECUTOR_CORES/MEMORY, WXD_SPARK_DRIVER_CORES/MEMORY,
  WXD_SPARK_WAIT_TIMEOUT_SEC. Auth/endpoint/Presto vars are read via common/wxd.py
  (WXD_CPD_HOST, WXD_API_KEY, WXD_INSTANCE_ID, WXD_SPARK_APPLICATIONS_ENDPOINT,
  WXD_SPARK_ENGINE_ID, WXD_HOST/PORT/CATALOG, WXD_SSL_VERIFY, …).

PREREQUISITES
  A running watsonx.data Spark engine + Presto coordinator reachable from the
  worker, the repo bind-mounted at PROJECT_DIR, a valid TLS CA cert, and the
  Spark assets present in the object store (see WHEN). No oc/cpdctl login needed.

USAGE
  airflow dags trigger spark_medallion_hourly
  airflow dags trigger spark_medallion_hourly -c '{"dry_run": true}'
  Tasks log the Spark app id + polled state, then per-layer row counts and a
  top-categories gold query, so the run is legible in the task logs.

SIDE EFFECTS / EXIT
  Submits a distributed Spark application that writes the
  spark_demo_{bronze,silver,gold} Iceberg tables (schemas pre-created via Presto
  to avoid `.db` dirs), polls it to completion, then runs read-only Presto
  verification + a business query. A non-zero Spark return_code, terminal state,
  or empty layer fails the run via AirflowException. dagrun_timeout = 60 min;
  the wait sensor caps at WXD_SPARK_WAIT_TIMEOUT_SEC (default 15 min, retries=0
  so a timeout never re-submits).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task, Param
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.standard.sensors.python import PythonSensor

try:
    from airflow.sdk import TaskGroup
except ImportError:  # pragma: no cover
    from airflow.utils.task_group import TaskGroup

from common import wxd

PROJECT_DIR = "/opt/airflow/project"

# Spark medallion schema names (built by load_medallion_demo.py).
SPARK_BASE_SCHEMA = os.getenv("WXD_SPARK_SCHEMA", "spark_demo")
BRONZE_SCHEMA = os.getenv("WXD_SPARK_BRONZE_SCHEMA", f"{SPARK_BASE_SCHEMA}_bronze")
SILVER_SCHEMA = os.getenv("WXD_SPARK_SILVER_SCHEMA", f"{SPARK_BASE_SCHEMA}_silver")
GOLD_SCHEMA = os.getenv("WXD_SPARK_GOLD_SCHEMA", f"{SPARK_BASE_SCHEMA}_gold")

# Tables written by the Spark job, per layer.
BRONZE_TABLES = [f"bronze_{e}" for e in ("customers", "products", "orders", "order_items")]
SILVER_TABLES = [
    "spark_silver_customers", "spark_silver_products", "spark_silver_orders",
    "spark_silver_order_items", "spark_silver_sales_enriched",
]
GOLD_TABLES = ["spark_gold_daily_sales", "spark_gold_category_performance", "spark_gold_customer_360"]

DRY_RUN_SENTINEL = "dry-run"


# ---------------------------------------------------------------------------
# Module-level sensor callable (must be importable by the scheduler).
# ---------------------------------------------------------------------------

def poll_spark_status(**context) -> bool:
    """PythonSensor poke: True when finished, raise on terminal failure."""
    import requests

    ti = context["ti"]
    app_id = ti.xcom_pull(task_ids="build.submit_spark_app")
    auth_header = ti.xcom_pull(task_ids="auth.get_token")

    if app_id == DRY_RUN_SENTINEL:
        return True

    resp = requests.get(
        f"{wxd.spark_applications_endpoint().rstrip('/')}/{app_id}",
        headers={"Authorization": auth_header, "LhInstanceId": wxd.instance_id()},
        verify=wxd.ssl_verify(),
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    # The v3 API returns UPPERCASE states (FINISHED/FAILED/RUNNING); v2 returns
    # lowercase. Normalise so terminal-state detection works on both.
    state = body.get("state", "unknown").lower()
    print(f"  Spark app {app_id}  state={state}")

    if state == "finished":
        rc = str(body.get("return_code", "0"))
        if rc != "0":
            raise AirflowException(f"Spark app finished with return_code={rc}")
        return True
    if state in {"failed", "error", "killed", "stopped"}:
        raise AirflowException(f"Spark app {app_id} ended in terminal state: {state}")
    return False  # accepted / waiting / running -> keep polling


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="spark_medallion_hourly",
    description="watsonx.data Spark medallion: submit one job, then verify each layer",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
    tags=["spark", "presto", "watsonx", "medallion"],
    params={
        "upload_assets": Param(
            False, type="boolean",
            description="Upload the Spark app + raw CSVs to MinIO first. "
                        "Needs the object store reachable from the container; "
                        "usually run once on the host instead.",
        ),
        "dry_run": Param(
            False, type="boolean",
            description="Build and print the Spark payload without submitting.",
        ),
    },
)
def spark_medallion_hourly():

    # === prepare ============================================================
    def _upload_enabled(**context) -> bool:
        return bool(context["params"]["upload_assets"])

    with TaskGroup(group_id="prepare") as prepare:
        gate_upload = ShortCircuitOperator(
            task_id="check_upload_enabled",
            python_callable=_upload_enabled,
            # Respect the submit task's trigger rule instead of cascade-skipping
            # it: when upload is disabled, only upload_assets is skipped.
            ignore_downstream_trigger_rules=False,
        )
        upload_assets = BashOperator(
            task_id="upload_assets",
            bash_command=f"cd {PROJECT_DIR} && python scripts/upload_spark_assets.py",
        )
        gate_upload >> upload_assets

    # === auth ===============================================================
    with TaskGroup(group_id="auth") as auth:
        @task(task_id="get_token")
        def get_token() -> str:
            """Mint a fresh CPD bearer token (shared logic in common/wxd.py)."""
            return wxd.bearer_auth_header()

        token = get_token()

    # === build (single Spark submission + wait) =============================
    with TaskGroup(group_id="build") as build:
        @task(task_id="submit_spark_app", trigger_rule="none_failed_min_one_success")
        def submit_spark_app(auth_header: str, **context) -> str:
            """Submit load_medallion_demo.py; return the application id."""
            import requests

            logical_date = context["logical_date"]
            batch_id = f"{SPARK_BASE_SCHEMA}_{logical_date.strftime('%Y%m%d_%H%M')}"

            application = os.getenv(
                "WXD_SPARK_APPLICATION",
                "s3a://iceberg-bucket/spark_demo/app/load_medallion_demo.py",
            )
            input_base = os.getenv("WXD_SPARK_INPUT_BASE", "s3a://iceberg-bucket/spark_demo/raw")
            catalog = os.getenv("WXD_SPARK_CATALOG", "iceberg_data")
            schema = SPARK_BASE_SCHEMA

            conf = {
                "spark.app.name": "watsonxdata-medallion-spark-demo",
                "spark.hadoop.wxd.apiKey": wxd.zen_api_key(),
                "spark.executor.cores": os.getenv("WXD_SPARK_EXECUTOR_CORES", "1"),
                "spark.executor.memory": os.getenv("WXD_SPARK_EXECUTOR_MEMORY", "2G"),
                "spark.driver.cores": os.getenv("WXD_SPARK_DRIVER_CORES", "1"),
                "spark.driver.memory": os.getenv("WXD_SPARK_DRIVER_MEMORY", "2G"),
            }
            # Pass medallion settings to driver + executors (every prefix the
            # engine honours), mirroring submit_spark_application.py.
            env_vars = {
                "WXD_SPARK_INPUT_BASE": input_base,
                "WXD_SPARK_CATALOG": catalog,
                "WXD_SPARK_SCHEMA": schema,
                "WXD_SPARK_INGEST_BATCH_ID": batch_id,
            }
            for prefix in ("spark.executorEnv", "spark.yarn.appMasterEnv",
                           "spark.driverEnv", "spark.kubernetes.driverEnv"):
                for k, v in env_vars.items():
                    conf[f"{prefix}.{k}"] = v

            payload = {"application_details": {"application": application, "conf": conf}}

            if context["params"]["dry_run"]:
                safe = {k: ("<redacted>" if k == "spark.hadoop.wxd.apiKey" else v)
                        for k, v in conf.items()}
                print(f"DRY RUN — would POST to {wxd.spark_applications_endpoint()}")
                print(f"  application: {application}")
                print(f"  batch_id:    {batch_id}")
                print(f"  conf:        {safe}")
                return DRY_RUN_SENTINEL

            # Pre-create the Spark namespaces via Presto so tables land at the
            # catalog default warehouse (bucket root, no Hive `.db` suffix). The
            # watsonx.data Iceberg catalog ignores CREATE NAMESPACE ... LOCATION,
            # so this Presto pre-create is the only way to avoid spark_demo_*.db/.
            try:
                conn = wxd.presto_connect()
                cur = conn.cursor()
                for s in (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA):
                    cur.execute(f"create schema if not exists {catalog}.{s}")
                    cur.fetchall()
                print(f"Pre-created Spark namespaces via Presto (no .db): "
                      f"{BRONZE_SCHEMA}, {SILVER_SCHEMA}, {GOLD_SCHEMA}")
            except Exception as exc:  # noqa: BLE001 — best-effort, never block submit
                print(f"WARNING: could not pre-create Spark schemas ({exc}); Spark may add .db dirs.")

            print(f"Submitting {application}  (batch_id={batch_id})")
            resp = requests.post(
                wxd.spark_applications_endpoint(),
                headers={
                    "Authorization": auth_header,
                    "Content-Type": "application/json",
                    "LhInstanceId": wxd.instance_id(),
                },
                json=payload,
                verify=wxd.ssl_verify(),
                timeout=60,
            )
            resp.raise_for_status()
            body = resp.json()
            app_id = body.get("id") or body.get("application_id") or body.get("application_uuid")
            if not app_id:
                raise AirflowException(f"No application id in Spark response: {body}")
            print(f"Submitted — app_id={app_id}  state={body.get('state')}")
            return app_id

        app_id = submit_spark_app(token)

        # Demo-friendly cap (default 15 min) — overridable via env so a slow
        # machine can extend it without editing the DAG. retries=0 here so a
        # timed-out sensor does NOT re-submit/re-poll a new Spark app; the
        # dagrun_timeout (60 min) is the whole-run backstop.
        spark_wait_timeout = int(os.getenv("WXD_SPARK_WAIT_TIMEOUT_SEC", "900"))
        wait = PythonSensor(
            task_id="wait_for_spark",
            python_callable=poll_spark_status,
            mode="reschedule",   # release the worker slot between pokes
            poke_interval=60,
            timeout=spark_wait_timeout,
            retries=0,
        )
        app_id >> wait

    # === verify (one Presto task per medallion layer) =======================
    def _count_layer(schema: str, tables: list[str]) -> None:
        """Count rows in each table of a layer via Presto and log a summary."""
        # In a real run wait_for_spark guarantees the tables are fresh; in a
        # dry run we still report whatever the last build left behind.
        total = 0
        for tbl in tables:
            try:
                n = wxd.presto_scalar(f'select count(*) from "{schema}"."{tbl}"')
            except Exception as exc:  # surface a missing table clearly
                raise AirflowException(f"{schema}.{tbl}: query failed: {exc}") from exc
            n = int(n or 0)
            total += n
            print(f"  {schema}.{tbl:32s} {n:>8,} rows")
        if total == 0:
            raise AirflowException(f"{schema}: all tables empty — Spark build looks wrong")
        print(f"{schema}: {total:,} rows across {len(tables)} tables")

    with TaskGroup(group_id="verify") as verify:
        @task(task_id="validate_bronze")
        def validate_bronze() -> None:
            _count_layer(BRONZE_SCHEMA, BRONZE_TABLES)

        @task(task_id="validate_silver")
        def validate_silver() -> None:
            _count_layer(SILVER_SCHEMA, SILVER_TABLES)

        @task(task_id="validate_gold")
        def validate_gold() -> None:
            _count_layer(GOLD_SCHEMA, GOLD_TABLES)

        v_bronze, v_silver, v_gold = validate_bronze(), validate_silver(), validate_gold()
        # Layers verify in medallion order.
        v_bronze >> v_silver >> v_gold

    # === query ==============================================================
    @task(task_id="query_gold_spark")
    def query_gold_spark() -> None:
        """A real business query on the Spark gold mart (top categories)."""
        conn = wxd.presto_connect(schema=GOLD_SCHEMA)
        try:
            cur = conn.cursor()
            cur.execute(
                "select category, total_orders, total_units, total_revenue "
                "from spark_gold_category_performance "
                "order by total_revenue desc limit 5"
            )
            rows = cur.fetchall()
        finally:
            conn.close()
        print("Top categories by revenue (Spark gold):")
        print(f"  {'CATEGORY':20s} {'ORDERS':>8s} {'UNITS':>8s} {'REVENUE':>14s}")
        for cat, orders, units, revenue in rows:
            print(f"  {str(cat):20s} {int(orders or 0):>8,} {int(units or 0):>8,} {float(revenue or 0):>14,.2f}")
        if not rows:
            raise AirflowSkipException("Gold category mart returned no rows")

    # === wiring =============================================================
    # upload (when enabled) must finish before submit; get_token feeds submit
    # via XCom (implicit). submit's trigger rule lets it run even if upload is
    # skipped. wait -> verify (per layer) -> final business query.
    upload_assets >> app_id
    build >> verify >> query_gold_spark()


spark_medallion_hourly()
