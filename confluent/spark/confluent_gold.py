#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  confluent_gold.py — PySpark job that builds the Confluent GOLD marts
#
#  Location  : confluent/spark/confluent_gold.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Builds the confluent_gold_* marts in
#      iceberg_data.{CONFLUENT_GOLD_SCHEMA} from the Flink-written Confluent
#      silver tables, mirroring spark/load_medallion_demo.py's gold logic 1:1.
#    v1.1 (2026-06-26) — Materialisation parity with dbt: gold_daily_sales stays
#      a physical TABLE; category_performance and customer_360 become catalog
#      VIEWS. Spark only writes the daily_sales table here — the two views are
#      created THROUGH PRESTO by the orchestrator (scripts/create_gold_views.py,
#      invoked from submit_confluent_gold.py after the app FINISHES), because a
#      Spark-created view is a Hive view that watsonx Presto cannot read.
# -----------------------------------------------------------------------------
"""PySpark job that builds the Confluent GOLD layer on watsonx.data Iceberg.

WHAT & WHY
----------
The Confluent track of this demo splits the medallion across two engines:

  * SILVER is streamed by **Flink** (Kafka -> Iceberg). It lands the cleaned,
    typed tables in ``iceberg_data.{CONFLUENT_SILVER_SCHEMA}``:
        confluent_silver_customers / confluent_silver_products /
        confluent_silver_orders   / confluent_silver_order_items /
        confluent_silver_sales_enriched
  * GOLD (this job) is built by **Spark**. It reads those Flink-written silver
    tables and materialises the three canonical business marts into
    ``iceberg_data.{CONFLUENT_GOLD_SCHEMA}``:
        confluent_gold_daily_sales
        confluent_gold_category_performance
        confluent_gold_customer_360

The whole point of the demo is PARITY: these gold marts use the *exact same*
business logic as the dbt path (the source of truth) and the Spark path
(``spark/load_medallion_demo.py``). This file is therefore a deliberate, line-
for-line mirror of that Spark job's GOLD section — same columns, same metric
math, same materialisation/partitioning — the ONLY difference being that the
silver source tables are the Confluent (Flink) ones instead of ``spark_silver_*``.
If the numbers in confluent_gold_* ever differ from spark_gold_* / dbt's gold_*,
the demo has a bug.

WHEN TO RUN
-----------
Run this AFTER the Flink silver pipeline has committed at least one checkpoint
AND the silver tables have been registered into watsonx.data (see
``confluent/scripts/prep_iceberg_schemas.py --phase register``), so that
``iceberg_data.{CONFLUENT_SILVER_SCHEMA}.confluent_silver_sales_enriched`` and
``...confluent_silver_customers`` are queryable from Spark. It is normally
launched on the watsonx.data Spark engine via
``confluent/scripts/submit_confluent_gold.py``.

ENV VARS (read at startup; a sibling .env is auto-loaded if python-dotenv is
installed). All schema/catalog values come from the environment — nothing is
hardcoded.
- WXD_SPARK_CATALOG        : Iceberg catalog name (default "iceberg_data").
- CONFLUENT_SILVER_SCHEMA  : Flink-written silver namespace
                             (default "confluent_demo_silver").
- CONFLUENT_GOLD_SCHEMA    : target gold namespace
                             (default "confluent_demo_gold").

PREREQUISITES
-------------
A running Spark session/engine wired to the watsonx.data Iceberg catalog and the
MinIO/S3 object store (e.g. a watsonx.data Spark application). No oc/cpdctl login
is performed here. As with the Spark medallion job, the watsonx.data Iceberg
catalog uses a fixed warehouse, so CREATE NAMESPACE ... LOCATION is ignored and
this job deliberately does not set a location.

USAGE
-----
    spark-submit confluent/spark/confluent_gold.py
    # or inside a configured pyspark environment:
    python3 confluent/spark/confluent_gold.py

SIDE EFFECTS & EXIT
-------------------
Creates the gold namespace if absent and writes confluent_gold_daily_sales as a
physical (month-partitioned) TABLE (createOrReplace). The other two marts —
confluent_gold_category_performance and confluent_gold_customer_360 — are catalog
VIEWS created separately THROUGH PRESTO by scripts/create_gold_views.py (a Spark
view is a Hive view that watsonx Presto cannot read), so this job intentionally
does NOT create them. Progress is printed per mart. The Spark session is stopped
before return; the process exits 0 on success and non-zero (after logging
context) on any failure.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


# On the host this file is confluent/spark/ -> parents[2] is the repo root. On
# the watsonx.data Spark engine the file is staged at a shallow work-dir path, so
# guard against there being fewer than 2 parents (ROOT is only used for the
# optional .env load; engine config comes from spark.executorEnv).
_self = Path(__file__).resolve()
ROOT = _self.parents[2] if len(_self.parents) > 2 else _self.parent

# Student-friendly logging: every line is prefixed and timestamped so the demo
# audience can follow exactly which mart is being written when.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [confluent_gold] %(levelname)s %(message)s",
)
log = logging.getLogger("confluent_gold")


def build_gold(spark: SparkSession, catalog: str, silver_schema: str, gold_schema: str) -> None:
    """Build the three Confluent gold marts from the Flink-written silver tables.

    This is a 1:1 mirror of the GOLD section of spark/load_medallion_demo.py.
    The only change is the *source*: we read confluent_silver_* (Flink output)
    instead of spark_silver_*. Keep the logic identical so the parity contract
    (dbt == Spark == Confluent) holds.
    """
    # The watsonx.data Spark engine's Iceberg catalog uses a fixed warehouse, so
    # a managed namespace always lands at <bucket-root>/<schema>.db/ and a
    # CREATE NAMESPACE ... LOCATION clause is ignored — so we do not set one.
    log.info("Ensuring gold namespace %s.%s", catalog, gold_schema)
    spark.sql(f"create namespace if not exists {catalog}.{gold_schema}")

    # ---- Sources: the Flink-written Confluent silver layer --------------------
    # confluent_silver_sales_enriched is the order-line-grain fact (the same
    # shape as spark_silver_sales_enriched), and confluent_silver_customers is
    # the customer dimension we LEFT-join back so customers with no orders still
    # appear in customer_360.
    enriched = spark.table(f"{catalog}.{silver_schema}.confluent_silver_sales_enriched")

    # ---- Gold mart 1: daily sales (physical, month-partitioned table) ---------
    # Only "completed" orders count toward revenue; grouped by day + category.
    daily_sales = (
        enriched.where(F.col("status") == "completed")
        .groupBy("order_date", "category")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("net_amount").cast("decimal(14,2)").alias("net_revenue"),
        )
    )
    log.info(
        "Writing partitioned gold daily sales table %s.%s.confluent_gold_daily_sales",
        catalog, gold_schema,
    )
    (
        daily_sales.writeTo(f"{catalog}.{gold_schema}.confluent_gold_daily_sales")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .partitionedBy(F.months("order_date"))
        .createOrReplace()
    )

    # ---- Gold marts 2 & 3: category_performance + customer_360 are VIEWS -------
    # Parity with dbt: those two marts are catalog VIEWS, not physical tables.
    # They are NOT created here, because Spark's CREATE VIEW makes a *Hive* view
    # that watsonx Presto refuses to read ("Hive views are not supported"). The
    # views are instead created by the orchestrator THROUGH PRESTO (dbt's exact
    # dialect, guaranteed Presto-readable) right after this Spark job writes the
    # confluent_gold_daily_sales table — see scripts/create_gold_views.py, called
    # from confluent/scripts/submit_confluent_gold.py once the app FINISHES.
    #
    # Clean any object a prior run left at those two names (a physical table from
    # the old design, or a Hive view from a transitional run) so the orchestrator's
    # Presto CREATE VIEW lands on a free name. Spark can drop BOTH kinds; each drop
    # is guarded because DROP TABLE on a view (or DROP VIEW on a table) raises.
    for _name in (
        f"{catalog}.{gold_schema}.confluent_gold_category_performance",
        f"{catalog}.{gold_schema}.confluent_gold_customer_360",
    ):
        for _ddl in (f"DROP VIEW IF EXISTS {_name}", f"DROP TABLE IF EXISTS {_name}"):
            try:
                spark.sql(_ddl)
            except Exception as _exc:  # name holds the *other* object type — ignore
                log.debug("  (%s) ignored: %s", _ddl, _exc)
    log.info(
        "Gold daily_sales TABLE written; stale view/table names cleared. "
        "category_performance + customer_360 are created as Presto VIEWS by the "
        "orchestrator (scripts/create_gold_views.py)."
    )


def main() -> int:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    # All identifiers come from the environment — nothing hardcoded.
    catalog = os.getenv("WXD_SPARK_CATALOG", "iceberg_data")
    silver_schema = os.getenv("CONFLUENT_SILVER_SCHEMA", "confluent_demo_silver")
    gold_schema = os.getenv("CONFLUENT_GOLD_SCHEMA", "confluent_demo_gold")

    log.info("Starting Confluent GOLD build (Spark over Flink-written silver)")
    log.info("Spark catalog       : %s", catalog)
    log.info("Confluent silver    : %s", silver_schema)
    log.info("Confluent gold (out): %s", gold_schema)

    spark = None
    try:
        spark = SparkSession.builder.appName("watsonxdata-confluent-gold").getOrCreate()
        build_gold(spark, catalog, silver_schema, gold_schema)
        log.info(
            "[OK] Confluent gold build complete — confluent_gold_* written to %s.%s",
            catalog, gold_schema,
        )
        return 0
    except Exception:  # noqa: BLE001 — log full context then fail loudly
        log.exception(
            "Confluent gold build FAILED (catalog=%s, silver=%s, gold=%s). "
            "Check that the Flink silver tables exist and are registered in watsonx.data.",
            catalog, silver_schema, gold_schema,
        )
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
