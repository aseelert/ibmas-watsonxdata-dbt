#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  reconcile_gold.py — prove the dbt / Spark / Confluent gold marts are identical
#
#  Location  : scripts/reconcile_gold.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Symmetric-EXCEPT 3-way reconciliation
#      of the canonical gold marts (daily_sales / category_performance /
#      customer_360) across the dbt, Spark, and Confluent paths, with a clear
#      PASS/FAIL parity table and explicit per-engine name map.
# -----------------------------------------------------------------------------
"""Reconcile the gold marts across the three medallion engines.

WHAT / WHY
    The whole demo's promise is: the SAME 4 seed CSVs, run through three
    INDEPENDENT engines (dbt, Spark, Confluent's Flink+Spark/DataStage), produce
    the IDENTICAL gold result in the ``iceberg_data`` Iceberg catalog. This script
    is the proof. For each of the three canonical marts it runs a *symmetric*
    ``EXCEPT`` in BOTH directions between a reference path and every other path:

        (reference  EXCEPT  other)   -> rows in reference missing from other
        (other      EXCEPT  reference) -> rows in other missing from reference

    If BOTH counts are 0 the two marts hold the exact same SET of rows, so they
    are identical (the grain of every mart is one row per key, so set-equality is
    full equality). Any non-zero count is a real discrepancy and the script exits
    non-zero — making it safe to drop into CI or a pre-demo smoke check.

    dbt is the source of truth for the business logic, so when ``dbt`` is one of
    the selected paths it is used as the reference; otherwise the first path you
    list becomes the reference.

THE CANONICAL MARTS (grain + columns must match exactly across engines)
    - daily_sales ........... one row per order_date(+category bucket)
    - category_performance .. one row per product category
    - customer_360 .......... one row per customer (LEFT join, 0-filled)

NAME MAP (catalog ``iceberg_data``; all names come from .env — nothing hardcoded)
    path        schema (env var)                         table prefix
    --------    --------------------------------------   --------------
    dbt         WXD_GOLD_SCHEMA      (def dbt_demo_gold)  gold_
    spark       WXD_SPARK_GOLD_SCHEMA(def spark_demo_gold) spark_gold_
    confluent   CONFLUENT_GOLD_SCHEMA(def confluent_demo_gold) confluent_gold_

    e.g. the daily_sales mart is dbt_demo_gold.gold_daily_sales vs
    spark_demo_gold.spark_gold_daily_sales vs
    confluent_demo_gold.confluent_gold_daily_sales.

ENV VARS (read at runtime; .env is auto-loaded if python-dotenv is installed)
    - WXD_USER         (default ``ibmlhapikey_cpadmin``) — Presto principal.
    - WXD_API_KEY      (required) — IBM Cloud / CPD apikey used as the password.
    - WXD_HOST         (required) — Presto host name.
    - WXD_PORT         (default ``443``) — Presto HTTPS port.
    - WXD_CATALOG      (default ``iceberg_data``) — Iceberg catalog.
    - WXD_SCHEMA       (default ``dbt_demo``)   — dbt base schema name.
    - WXD_SPARK_SCHEMA (default ``spark_demo``) — Spark base schema name.
    - WXD_GOLD_SCHEMA / WXD_SPARK_GOLD_SCHEMA / CONFLUENT_GOLD_SCHEMA — gold
      schema overrides (defaults derived from the base names above).
    - WXD_SSL_VERIFY   (default ``certs/watsonxdata-ca.pem``) — True/False or a CA
      bundle path (relative paths resolve against the repo root).
    - WXD_INSTANCE_ID  (optional) — sent as the ``LhInstanceId`` HTTP header.

PREREQUISITES
    - ``presto-python-client`` installed (``pip install -r requirements.txt``).
    - A reachable, resumed watsonx.data Presto engine, and the gold marts for the
      selected paths already built (dbt run / Spark job / Confluent gold job).

USAGE
    python scripts/reconcile_gold.py                         # all three paths
    python scripts/reconcile_gold.py --paths dbt,spark       # just two
    python scripts/reconcile_gold.py --paths dbt,confluent

SIDE EFFECTS / EXIT
    - Read-only: issues only SELECT/EXCEPT queries, writes nothing. Prints the
      name map and a PASS/FAIL parity table. Exit 0 = all marts identical;
      exit 1 = at least one discrepancy (or a mart could not be read); exit 2 =
      bad arguments / missing env var / missing dependency.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any


try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("reconcile_gold")


# Canonical marts: key -> the EXACT ordered column projection that every engine's
# mart must expose. Projecting an explicit, shared column list (instead of
# SELECT *) makes the EXCEPT comparison immune to column-ORDER differences across
# engines while still catching any value difference. These columns mirror the
# dbt gold models (the source of truth).
MARTS: dict[str, list[str]] = {
    "daily_sales": [
        "order_date",
        "category",
        "order_count",
        "units_sold",
        "net_revenue",
    ],
    "category_performance": [
        "category",
        "total_orders",
        "total_units",
        "total_revenue",
        "avg_revenue_per_unit",
    ],
    "customer_360": [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "signup_date",
        "completed_orders",
        "returned_orders",
        "pending_orders",
        "cancelled_orders",
        "lifetime_value",
        "last_completed_order_ts",
        "last_activity_ts",
    ],
}

# The three engine "paths". table_prefix + mart key = the physical table name
# (e.g. "gold_" + "daily_sales" -> gold_daily_sales).
PATHS = ("dbt", "spark", "confluent")


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def _ssl_verify() -> bool | str:
    value = os.getenv("WXD_SSL_VERIFY", "certs/watsonxdata-ca.pem").strip()
    if value.lower() in {"0", "false", "no"}:
        return False
    if value.lower() in {"1", "true", "yes"}:
        return True

    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / path
    return str(path)


def _http_headers() -> dict[str, str] | None:
    instance_id = os.getenv("WXD_INSTANCE_ID", "").strip()
    if not instance_id:
        return None
    return {"LhInstanceId": instance_id}


def _resolve_paths(catalog: str) -> dict[str, dict[str, str]]:
    """Build, per engine path, its gold schema + table-name prefix (env-driven)."""
    dbt_base = os.getenv("WXD_SCHEMA", "dbt_demo")
    spark_base = os.getenv("WXD_SPARK_SCHEMA", "spark_demo")
    return {
        "dbt": {
            "schema": os.getenv("WXD_GOLD_SCHEMA", f"{dbt_base}_gold"),
            "prefix": "gold_",
        },
        "spark": {
            "schema": os.getenv("WXD_SPARK_GOLD_SCHEMA", f"{spark_base}_gold"),
            "prefix": "spark_gold_",
        },
        "confluent": {
            "schema": os.getenv("CONFLUENT_GOLD_SCHEMA", "confluent_demo_gold"),
            "prefix": "confluent_gold_",
        },
    }


def _fqn(catalog: str, path_cfg: dict[str, str], mart_key: str) -> str:
    """Fully-qualified table name: catalog.schema.<prefix><mart_key>."""
    return f"{catalog}.{path_cfg['schema']}.{path_cfg['prefix']}{mart_key}"


def _scalar(cur: Any, sql: str) -> int:
    cur.execute(sql)
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _except_count(cur: Any, cols: str, left_fqn: str, right_fqn: str) -> int:
    """Rows present in left_fqn but NOT in right_fqn (set difference)."""
    sql = (
        f"select count(*) from ("
        f"select {cols} from {left_fqn} "
        f"except "
        f"select {cols} from {right_fqn}"
        f") as _diff"
    )
    return _scalar(cur, sql)


def _print_name_map(catalog: str, resolved: dict[str, dict[str, str]], selected: list[str]) -> None:
    print("\nName map (catalog = " + catalog + ")")
    print("=" * 60)
    for mart_key in MARTS:
        print(f"  {mart_key}:")
        for path in selected:
            print(f"      {path:<10} {_fqn(catalog, resolved[path], mart_key)}")
    print()


def _print_parity_table(reference: str, rows: list[dict[str, Any]]) -> None:
    """Render the PASS/FAIL parity table. rows = list of result dicts."""
    headers = ["mart", "pair", f"{reference}-only", "other-only", "result"]
    table = [headers]
    for r in rows:
        table.append([
            r["mart"],
            f"{reference} vs {r['other']}",
            r.get("ref_only", "ERR") if r["status"] != "ERROR" else "ERR",
            r.get("other_only", "ERR") if r["status"] != "ERROR" else "ERR",
            r["status"],
        ])
    table = [[str(c) for c in row] for row in table]
    widths = [max(len(row[i]) for row in table) for i in range(len(headers))]

    def line(row: list[str]) -> str:
        return "| " + " | ".join(row[i].ljust(widths[i]) for i in range(len(row))) + " |"

    border = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    print("Reconciliation parity table (reference = " + reference + ")")
    print(border)
    print(line(table[0]))
    print(border)
    for row in table[1:]:
        print(line(row))
    print(border)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile the gold marts across the dbt, Spark and Confluent paths.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--paths",
        default=",".join(PATHS),
        help="Comma-separated subset of paths to compare "
        f"(choices: {', '.join(PATHS)}). Default: all three. "
        "At least two are required. dbt is used as the reference when present.",
    )
    return parser.parse_args()


def _select_paths(raw: str) -> list[str]:
    requested = [p.strip().lower() for p in raw.split(",") if p.strip()]
    unknown = [p for p in requested if p not in PATHS]
    if unknown:
        raise SystemExit(
            f"Unknown path(s): {', '.join(unknown)}. Valid choices: {', '.join(PATHS)}."
        )
    # De-duplicate, preserve the canonical PATHS order for stable output.
    selected = [p for p in PATHS if p in requested]
    if len(selected) < 2:
        raise SystemExit("Need at least two distinct paths to reconcile (e.g. --paths dbt,spark).")
    return selected


def main() -> int:
    args = _parse_args()
    selected = _select_paths(args.paths)
    # dbt is the source of truth — use it as the reference whenever it is selected.
    reference = "dbt" if "dbt" in selected else selected[0]
    others = [p for p in selected if p != reference]

    if load_dotenv is not None:
        load_dotenv()

    try:
        import prestodb
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'presto-python-client'. Install dependencies with: "
            "python -m pip install -r requirements.txt"
        ) from exc

    user = _env("WXD_USER", "ibmlhapikey_cpadmin")
    password = _env("WXD_API_KEY")
    host = _env("WXD_HOST")
    port = int(_env("WXD_PORT", "443"))
    catalog = _env("WXD_CATALOG", "iceberg_data")

    resolved = _resolve_paths(catalog)
    _print_name_map(catalog, resolved, selected)

    log.info("Connecting to Presto %s:%s (catalog=%s) ...", host, port, catalog)
    conn = prestodb.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        http_scheme="https",
        http_headers=_http_headers(),
        auth=prestodb.auth.BasicAuthentication(user, password),
        request_timeout=60,
    )
    conn._http_session.verify = _ssl_verify()
    cur = conn.cursor()
    log.info("Connected. Reconciling %d mart(s) for: %s (reference=%s)",
             len(MARTS), ", ".join(selected), reference)

    results: list[dict[str, Any]] = []
    failures = 0
    for other in others:
        for mart_key, columns in MARTS.items():
            cols = ", ".join(columns)
            ref_fqn = _fqn(catalog, resolved[reference], mart_key)
            other_fqn = _fqn(catalog, resolved[other], mart_key)
            try:
                ref_only = _except_count(cur, cols, ref_fqn, other_fqn)
                other_only = _except_count(cur, cols, other_fqn, ref_fqn)
                status = "PASS" if (ref_only == 0 and other_only == 0) else "FAIL"
                if status == "FAIL":
                    failures += 1
                    log.warning(
                        "FAIL %s: %s vs %s — %d ref-only / %d other-only row(s) differ",
                        mart_key, reference, other, ref_only, other_only,
                    )
                results.append({
                    "mart": mart_key,
                    "other": other,
                    "ref_only": ref_only,
                    "other_only": other_only,
                    "status": status,
                })
            except Exception as exc:  # noqa: BLE001 — surface any query error per-mart
                failures += 1
                log.error("ERROR %s: %s vs %s — %s", mart_key, reference, other, exc)
                results.append({
                    "mart": mart_key,
                    "other": other,
                    "status": "ERROR",
                })

    print()
    _print_parity_table(reference, results)

    total = len(results)
    passed = total - failures
    print()
    if failures == 0:
        log.info("PARITY PROVEN: all %d comparison(s) identical across %s.",
                 total, ", ".join(selected))
        return 0
    log.error("PARITY BROKEN: %d/%d comparison(s) failed (only %d passed).",
              failures, total, passed)
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except KeyboardInterrupt:
        log.error("Interrupted.")
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001 — top-level safety net
        log.exception("Unexpected failure: %s", exc)
        sys.exit(2)
