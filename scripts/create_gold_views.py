#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  create_gold_views.py — create the two GOLD "view" marts THROUGH PRESTO
#
#  Location  : scripts/create_gold_views.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Creates category_performance and
#      customer_360 as catalog VIEWS for the Spark and Confluent gold paths, so
#      their materialisation matches dbt (which makes both marts VIEWs). The
#      views are created via Presto — NOT via Spark — because a Spark CREATE VIEW
#      produces a Hive view that watsonx Presto refuses to read ("Hive views are
#      not supported"). The SQL mirrors models/gold/gold_category_performance.sql
#      and models/gold/gold_customer_360.sql verbatim, so a cross-path reconcile
#      stays byte-for-byte equal. Idempotent: drops any pre-existing table OR
#      view at each name first. Importable (create_views) + CLI (--path).
# -----------------------------------------------------------------------------
"""Create the gold "view" marts (category_performance, customer_360) via Presto.

WHY THIS EXISTS (for an 18-year-old learner)
--------------------------------------------
In the dbt path, two gold marts are VIEWS — thin saved queries that are computed
on demand, storing no extra data:
    gold_category_performance   (a per-category roll-up of gold_daily_sales)
    gold_customer_360           (one row per customer: lifetime metrics)
The Spark and Confluent paths build their gold *table* (daily_sales) with Spark,
but Spark cannot create a view that watsonx **Presto** can read — Spark makes a
"Hive view" and Presto answers "Hive views are not supported". So we create these
two views with **Presto itself**, using the SAME SQL as the dbt models. That way
all three paths agree not just on the rows, but on the SHAPE (table vs view).

WHAT IT DOES
------------
For the chosen --path (spark | confluent) it:
  1. Connects to Presto (ZenApiKey BasicAuth, LhInstanceId header, TLS CA).
  2. For each of the two marts: DROP whatever exists at that name (table OR
     view), then CREATE VIEW with the dbt SQL pointed at that path's schemas.
It is safe to re-run (idempotent) and never touches gold_daily_sales (a TABLE).

ENV VARS (read at startup; .env auto-loaded if python-dotenv is installed)
-------------------------------------------------------------------------
  Presto conn : WXD_HOST, WXD_PORT (443), WXD_USER (ibmlhapikey_cpadmin),
                WXD_API_KEY, WXD_INSTANCE_ID, WXD_SSL_VERIFY (CA path / "true")
  Catalog     : WXD_SPARK_CATALOG (default "iceberg_data")
  confluent   : CONFLUENT_GOLD_SCHEMA   (default "confluent_demo_gold")
                CONFLUENT_SILVER_SCHEMA (default "confluent_demo_silver")
  spark       : WXD_SPARK_SCHEMA (base, default "spark_demo") →
                <base>_gold / <base>_silver, or explicit WXD_SPARK_GOLD_SCHEMA /
                WXD_SPARK_SILVER_SCHEMA overrides.

USAGE
-----
    python scripts/create_gold_views.py --path confluent
    python scripts/create_gold_views.py --path spark
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

try:
    from dotenv import load_dotenv
except ImportError:  # python-dotenv is optional
    load_dotenv = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [create_gold_views] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("create_gold_views")


def _ssl_verify() -> object:
    """Return the value for requests' ``verify`` (CA path, or True/False)."""
    value = os.getenv("WXD_SSL_VERIFY", "certs/watsonxdata-ca.pem").strip()
    if value.lower() in {"true", "1", "yes"}:
        return True
    if value.lower() in {"false", "0", "no"}:
        return False
    return value  # treat as a CA bundle path


def connect():
    """Open a Presto DB-API connection using the demo's ZenApiKey auth."""
    import prestodb  # imported lazily so --help works without the dep

    host = os.environ["WXD_HOST"]
    port = int(os.getenv("WXD_PORT", "443"))
    user = os.getenv("WXD_USER", "ibmlhapikey_cpadmin")
    catalog = os.getenv("WXD_SPARK_CATALOG", "iceberg_data")
    conn = prestodb.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        http_scheme="https",
        auth=prestodb.auth.BasicAuthentication(user, os.environ["WXD_API_KEY"]),
        http_headers={"LhInstanceId": os.getenv("WXD_INSTANCE_ID", "")},
    )
    conn._http_session.verify = _ssl_verify()
    log.info("Connected to Presto %s:%s (catalog=%s)", host, port, catalog)
    return conn


def _category_performance_sql(catalog: str, gold_schema: str, prefix: str) -> str:
    """dbt's gold_category_performance, in Presto SQL, over <prefix>gold_daily_sales."""
    return f"""
CREATE VIEW {catalog}.{gold_schema}.{prefix}gold_category_performance AS
SELECT
  category,
  sum(order_count) AS total_orders,
  sum(units_sold) AS total_units,
  cast(sum(net_revenue) AS decimal(14, 2)) AS total_revenue,
  cast(sum(net_revenue) / nullif(sum(units_sold), 0) AS decimal(14, 2)) AS avg_revenue_per_unit
FROM {catalog}.{gold_schema}.{prefix}gold_daily_sales
GROUP BY 1
""".strip()


def _customer_360_sql(catalog: str, gold_schema: str, silver_schema: str, prefix: str) -> str:
    """dbt's gold_customer_360, in Presto SQL, over the <prefix>silver_* tables."""
    return f"""
CREATE VIEW {catalog}.{gold_schema}.{prefix}gold_customer_360 AS
WITH metrics AS (
  SELECT
    customer_id,
    count(distinct case when status = 'completed' then order_id end) AS completed_orders,
    count(distinct case when status = 'returned' then order_id end) AS returned_orders,
    count(distinct case when status = 'pending' then order_id end) AS pending_orders,
    count(distinct case when status = 'cancelled' then order_id end) AS cancelled_orders,
    cast(coalesce(sum(case when status = 'completed' then net_amount else 0 end), 0) as decimal(14, 2)) AS lifetime_value,
    max(case when status = 'completed' then order_ts end) AS last_completed_order_ts,
    max(order_ts) AS last_activity_ts
  FROM {catalog}.{silver_schema}.{prefix}silver_sales_enriched
  GROUP BY customer_id
)
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.country,
  c.signup_date,
  coalesce(m.completed_orders, 0) AS completed_orders,
  coalesce(m.returned_orders, 0) AS returned_orders,
  coalesce(m.pending_orders, 0) AS pending_orders,
  coalesce(m.cancelled_orders, 0) AS cancelled_orders,
  coalesce(m.lifetime_value, 0) AS lifetime_value,
  m.last_completed_order_ts,
  m.last_activity_ts
FROM {catalog}.{silver_schema}.{prefix}silver_customers c
LEFT JOIN metrics m
  ON c.customer_id = m.customer_id
""".strip()


def _drop_any(cur, fqn: str) -> None:
    """Drop whatever object exists at ``fqn`` (a TABLE or a VIEW), idempotently.

    Each DROP is guarded: DROP TABLE on a view (or DROP VIEW on a table) raises in
    Presto, which we ignore — only one of the two applies to whatever is there.
    """
    for ddl in (f"DROP VIEW IF EXISTS {fqn}", f"DROP TABLE IF EXISTS {fqn}"):
        try:
            cur.execute(ddl)
            cur.fetchall()
        except Exception as exc:  # wrong object type for this DROP — ignore
            log.debug("  (%s) ignored: %s", ddl, str(exc)[:80])


def create_views(conn, catalog: str, gold_schema: str, silver_schema: str, prefix: str) -> list[str]:
    """Create the two gold VIEW marts for one path. Returns the FQNs created."""
    cur = conn.cursor()
    created: list[str] = []
    targets = [
        (f"{catalog}.{gold_schema}.{prefix}gold_category_performance",
         _category_performance_sql(catalog, gold_schema, prefix)),
        (f"{catalog}.{gold_schema}.{prefix}gold_customer_360",
         _customer_360_sql(catalog, gold_schema, silver_schema, prefix)),
    ]
    for fqn, create_sql in targets:
        log.info("Recreating VIEW %s", fqn)
        _drop_any(cur, fqn)
        cur.execute(create_sql)
        cur.fetchall()
        created.append(fqn)
        log.info("  created VIEW %s", fqn)
    return created


def _resolve_schemas(path: str) -> tuple[str, str, str]:
    """Return (gold_schema, silver_schema, table_prefix) for the chosen path."""
    if path == "confluent":
        gold = os.getenv("CONFLUENT_GOLD_SCHEMA", "confluent_demo_gold")
        silver = os.getenv("CONFLUENT_SILVER_SCHEMA", "confluent_demo_silver")
        return gold, silver, "confluent_"
    if path == "spark":
        base = os.getenv("WXD_SPARK_SCHEMA", "spark_demo")
        gold = os.getenv("WXD_SPARK_GOLD_SCHEMA", f"{base}_gold")
        silver = os.getenv("WXD_SPARK_SILVER_SCHEMA", f"{base}_silver")
        return gold, silver, "spark_"
    raise ValueError(f"unknown --path '{path}' (expected 'spark' or 'confluent')")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create gold category_performance + customer_360 VIEWS via Presto.",
    )
    parser.add_argument(
        "--path", required=True, choices=["spark", "confluent"],
        help="Which gold path's views to (re)create.",
    )
    args = parser.parse_args(argv)

    if load_dotenv is not None:
        load_dotenv()

    catalog = os.getenv("WXD_SPARK_CATALOG", "iceberg_data")
    try:
        gold_schema, silver_schema, prefix = _resolve_schemas(args.path)
    except ValueError as exc:
        log.error("%s", exc)
        return 2

    log.info(
        "Path '%s' → gold=%s silver=%s prefix=%s (catalog=%s)",
        args.path, gold_schema, silver_schema, prefix, catalog,
    )

    try:
        conn = connect()
    except KeyError as exc:
        log.error("Missing required env var: %s (source .env / run prepare_watsonx_env.py)", exc)
        return 1
    except ImportError:
        log.error("prestodb not installed — run: pip install -r requirements.txt")
        return 1
    except Exception as exc:  # network/auth/TLS
        log.error("Could not connect to Presto: %s", exc)
        return 1

    try:
        created = create_views(conn, catalog, gold_schema, silver_schema, prefix)
    except Exception as exc:
        log.error("Failed to create gold views: %s", exc)
        return 1

    log.info("Done — %d view(s) created: %s", len(created), ", ".join(created))
    return 0


if __name__ == "__main__":
    sys.exit(main())
