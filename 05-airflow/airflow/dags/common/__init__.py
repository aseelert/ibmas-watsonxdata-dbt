# -----------------------------------------------------------------------------
#  __init__.py — Package marker for the shared Airflow DAG helpers
#
#  Location  : airflow/dags/common/__init__.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Marks airflow/dags/common as an importable package so the DAGs can do
#  `from common import wxd` for shared watsonx.data auth/TLS/connection logic.
# -----------------------------------------------------------------------------
"""Shared helpers for the watsonx.data Airflow DAGs."""
