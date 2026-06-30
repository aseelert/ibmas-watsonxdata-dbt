#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  create_medallion_flows.py — build + create the DataStage medallion flows in CPD
#
#  Location  : scripts/datastage/create_medallion_flows.py
#  Project   : watsonx.data · dbt · Spark · Confluent · DataStage medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  WHAT
#  ----
#  A fourth, interchangeable medallion path alongside dbt / Spark / cpdctl:
#  three IBM DataStage flows (bronze, silver, gold) that reproduce the dbt models
#  EXACTLY, pushing every transformation down to the watsonx.data Presto engine as
#  SQL through ONE "IBM watsonx.data Presto" connection (DataStage op="lakehouse").
#
#      CSV (already landed as Iceberg in dbt_demo_raw on MinIO)
#        --bronze flow-->  datastage_demo_bronze.*   (raw + ingest metadata)
#        --silver flow-->  datastage_demo_silver.*   (cast / clean / filter / join)
#        --gold   flow-->  datastage_demo_gold.*     (business aggregates)
#
#  Each dbt model is one source->target connector pair (see ds_flow_lib.py). No
#  DataStage Python SDK exists or is needed: a flow is plain pipeline-flow v3 JSON
#  POSTed to the Watson Data REST API.
#
#  USAGE
#  -----
#    python scripts/datastage/create_medallion_flows.py --build        # write JSON only
#    python scripts/datastage/create_medallion_flows.py --create       # build + POST to CPD
#    python scripts/datastage/create_medallion_flows.py --verify       # Presto parity vs dbt
#    python scripts/datastage/create_medallion_flows.py --create --verify
#
#  Auth/env come from .env (same vars dbt/Presto use). A CPD bearer token is
#  minted from WXD_API_KEY exactly like scripts/get_token.py.
# -----------------------------------------------------------------------------
from __future__ import annotations
import argparse
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ds_flow_lib import FlowBuilder  # noqa: E402

requests.packages.urllib3.disable_warnings()

ROOT = Path(__file__).resolve().parents[2]
FLOWS_DIR = Path(__file__).resolve().parent / "flows"
load_dotenv(ROOT / ".env")

PROJECT_ID = "2d2415ea-71b5-4215-a7b6-b32a4889611e"           # ibmas-ingest-demo
PRESTO_CONNECTION = "d23d59d4-18c3-4f31-a6b9-fd6f59304c14"    # ibmas-presto (watsonx.data Presto)
CATALOG = "iceberg_data"
RAW = "dbt_demo_raw"          # shared raw landing (CSV already ingested into Iceberg)
BRONZE = "datastage_demo_bronze"
SILVER = "datastage_demo_silver"
GOLD = "datastage_demo_gold"

# Ingest-metadata literals stamped by the bronze layer (parallel to dbt's bronze).
ING_BY = "datastage flow"
ING_BATCH = "datastage_seed_batch"

# --- shared cleaning CTEs (verbatim silver transforms, used by enriched) -----
_CLEAN = {
    "oi": (f"SELECT cast(order_item_id as integer) order_item_id, cast(order_id as integer) order_id, "
           f"cast(product_id as integer) product_id, cast(quantity as integer) quantity, "
           f"cast(discount_pct as decimal(5,2)) discount_pct "
           f"FROM {CATALOG}.{BRONZE}.bronze_order_items WHERE quantity > 0"),
    "o": (f"SELECT cast(order_id as integer) order_id, cast(customer_id as integer) customer_id, "
          f"cast(order_ts as timestamp) order_ts, cast(cast(order_ts as timestamp) as date) order_date, "
          f"lower(trim(status)) status, lower(trim(payment_method)) payment_method "
          f"FROM {CATALOG}.{BRONZE}.bronze_orders WHERE order_id IS NOT NULL"),
    "p": (f"SELECT cast(product_id as integer) product_id, trim(product_name) product_name, "
          f"trim(category) category, cast(unit_price as decimal(12,2)) unit_price "
          f"FROM {CATALOG}.{BRONZE}.bronze_products WHERE product_id IS NOT NULL"),
    "c": (f"SELECT cast(customer_id as integer) customer_id, trim(first_name) first_name, "
          f"trim(last_name) last_name, lower(trim(email)) email, cast(signup_date as date) signup_date, "
          f"upper(trim(country)) country FROM {CATALOG}.{BRONZE}.bronze_customers WHERE email IS NOT NULL"),
}

TS = "cast(current_timestamp as timestamp)"   # plain timestamp (DataStage has no tz type)


# =============================================================================
#  MODEL DEFINITIONS — one dict per dbt model: (label, schema, table, cols, sql)
# =============================================================================
def _bronze(table, raw_table, business_cols, src_file):
    """Bronze = raw passthrough + 4 ingest-metadata columns (mirrors dbt bronze)."""
    sel_cols = ", ".join(c[0] for c in business_cols)
    sql = (f"SELECT {sel_cols}, "
           f"{TS} AS _ingested_at, '{ING_BY}' AS _ingested_by, "
           f"'{src_file}' AS _source_file, '{ING_BATCH}' AS _ingest_batch_id "
           f"FROM {CATALOG}.{RAW}.{raw_table}")
    cols = business_cols + [("_ingested_at", "timestamp"), ("_ingested_by", "varchar"),
                            ("_source_file", "varchar"), ("_ingest_batch_id", "varchar")]
    return dict(label=table, target_schema=BRONZE, target_table=table, columns=cols, select_statement=sql)


BRONZE_MODELS = [
    _bronze("bronze_customers", "raw_customers",
            [("customer_id", "integer"), ("first_name", "varchar"), ("last_name", "varchar"),
             ("email", "varchar"), ("signup_date", "date"), ("country", "varchar")], "raw_customers.csv"),
    _bronze("bronze_orders", "raw_orders",
            [("order_id", "integer"), ("customer_id", "integer"), ("order_ts", "timestamp"),
             ("status", "varchar"), ("payment_method", "varchar")], "raw_orders.csv"),
    _bronze("bronze_order_items", "raw_order_items",
            [("order_item_id", "integer"), ("order_id", "integer"), ("product_id", "integer"),
             ("quantity", "integer"), ("discount_pct", "double")], "raw_order_items.csv"),
    _bronze("bronze_products", "raw_products",
            [("product_id", "integer"), ("product_name", "varchar"), ("category", "varchar"),
             ("unit_price", "double")], "raw_products.csv"),
]

SILVER_MODELS = [
    dict(label="silver_customers", target_schema=SILVER, target_table="silver_customers",
         columns=[("customer_id", "integer"), ("first_name", "varchar"), ("last_name", "varchar"),
                  ("email", "varchar"), ("signup_date", "date"), ("country", "varchar"),
                  ("transformed_at", "timestamp")],
         select_statement=(f"SELECT cast(customer_id as integer) customer_id, trim(first_name) first_name, "
                           f"trim(last_name) last_name, lower(trim(email)) email, "
                           f"cast(signup_date as date) signup_date, upper(trim(country)) country, "
                           f"{TS} transformed_at FROM {CATALOG}.{BRONZE}.bronze_customers WHERE email IS NOT NULL")),
    dict(label="silver_orders", target_schema=SILVER, target_table="silver_orders",
         columns=[("order_id", "integer"), ("customer_id", "integer"), ("order_ts", "timestamp"),
                  ("order_date", "date"), ("status", "varchar"), ("payment_method", "varchar"),
                  ("transformed_at", "timestamp")],
         select_statement=(f"SELECT cast(order_id as integer) order_id, cast(customer_id as integer) customer_id, "
                           f"cast(order_ts as timestamp) order_ts, cast(cast(order_ts as timestamp) as date) order_date, "
                           f"lower(trim(status)) status, lower(trim(payment_method)) payment_method, "
                           f"{TS} transformed_at FROM {CATALOG}.{BRONZE}.bronze_orders WHERE order_id IS NOT NULL")),
    dict(label="silver_order_items", target_schema=SILVER, target_table="silver_order_items",
         columns=[("order_item_id", "integer"), ("order_id", "integer"), ("product_id", "integer"),
                  ("quantity", "integer"), ("discount_pct", "decimal(5,2)"), ("transformed_at", "timestamp")],
         select_statement=(f"SELECT cast(order_item_id as integer) order_item_id, cast(order_id as integer) order_id, "
                           f"cast(product_id as integer) product_id, cast(quantity as integer) quantity, "
                           f"cast(discount_pct as decimal(5,2)) discount_pct, {TS} transformed_at "
                           f"FROM {CATALOG}.{BRONZE}.bronze_order_items WHERE quantity > 0")),
    dict(label="silver_products", target_schema=SILVER, target_table="silver_products",
         columns=[("product_id", "integer"), ("product_name", "varchar"), ("category", "varchar"),
                  ("unit_price", "decimal(12,2)"), ("transformed_at", "timestamp")],
         select_statement=(f"SELECT cast(product_id as integer) product_id, trim(product_name) product_name, "
                           f"trim(category) category, cast(unit_price as decimal(12,2)) unit_price, "
                           f"{TS} transformed_at FROM {CATALOG}.{BRONZE}.bronze_products WHERE product_id IS NOT NULL")),
    # enriched: inline the 4 clean CTEs over bronze, then the 3 INNER joins (verbatim dbt logic),
    # so this stage depends only on bronze and is safe to run in parallel with the 4 above.
    dict(label="silver_sales_enriched", target_schema=SILVER, target_table="silver_sales_enriched",
         columns=[("order_item_id", "integer"), ("order_id", "integer"), ("order_date", "date"),
                  ("order_ts", "timestamp"), ("status", "varchar"), ("payment_method", "varchar"),
                  ("customer_id", "integer"), ("customer_country", "varchar"), ("product_id", "integer"),
                  ("product_name", "varchar"), ("category", "varchar"), ("quantity", "integer"),
                  ("unit_price", "decimal(12,2)"), ("discount_pct", "decimal(5,2)"),
                  ("gross_amount", "decimal(14,2)"), ("net_amount", "decimal(14,2)"),
                  ("transformed_at", "timestamp")],
         select_statement=(
             f"WITH oi AS ({_CLEAN['oi']}), o AS ({_CLEAN['o']}), "
             f"p AS ({_CLEAN['p']}), c AS ({_CLEAN['c']}) "
             f"SELECT oi.order_item_id, oi.order_id, o.order_date, o.order_ts, o.status, o.payment_method, "
             f"c.customer_id, c.country AS customer_country, p.product_id, p.product_name, p.category, "
             f"oi.quantity, p.unit_price, oi.discount_pct, "
             f"cast(oi.quantity * p.unit_price as decimal(14,2)) gross_amount, "
             f"cast(oi.quantity * p.unit_price * (1 - oi.discount_pct) as decimal(14,2)) net_amount, "
             f"{TS} transformed_at "
             f"FROM oi JOIN o ON oi.order_id = o.order_id "
             f"JOIN p ON oi.product_id = p.product_id "
             f"JOIN c ON o.customer_id = c.customer_id")),
]

GOLD_MODELS = [
    dict(label="gold_daily_sales", target_schema=GOLD, target_table="gold_daily_sales",
         columns=[("order_date", "date"), ("category", "varchar"), ("order_count", "bigint"),
                  ("units_sold", "bigint"), ("net_revenue", "decimal(14,2)")],
         select_statement=(f"SELECT order_date, category, count(distinct order_id) order_count, "
                           f"sum(quantity) units_sold, cast(sum(net_amount) as decimal(14,2)) net_revenue "
                           f"FROM {CATALOG}.{SILVER}.silver_sales_enriched WHERE status = 'completed' "
                           f"GROUP BY 1, 2")),
    # category_performance reads the enriched fact directly (equivalent to summing daily_sales:
    # each order_id maps to a single order_date, so summing per-day distinct counts == per-category
    # distinct count). Avoids an intra-flow dependency on gold_daily_sales.
    dict(label="gold_category_performance", target_schema=GOLD, target_table="gold_category_performance",
         columns=[("category", "varchar"), ("total_orders", "bigint"), ("total_units", "bigint"),
                  ("total_revenue", "decimal(14,2)"), ("avg_revenue_per_unit", "decimal(14,2)")],
         select_statement=(f"SELECT category, count(distinct order_id) total_orders, "
                           f"sum(quantity) total_units, cast(sum(net_amount) as decimal(14,2)) total_revenue, "
                           f"cast(sum(net_amount) / nullif(sum(quantity), 0) as decimal(14,2)) avg_revenue_per_unit "
                           f"FROM {CATALOG}.{SILVER}.silver_sales_enriched WHERE status = 'completed' "
                           f"GROUP BY category")),
    dict(label="gold_customer_360", target_schema=GOLD, target_table="gold_customer_360",
         columns=[("customer_id", "integer"), ("first_name", "varchar"), ("last_name", "varchar"),
                  ("email", "varchar"), ("country", "varchar"), ("signup_date", "date"),
                  ("completed_orders", "bigint"), ("returned_orders", "bigint"),
                  ("pending_orders", "bigint"), ("cancelled_orders", "bigint"),
                  ("lifetime_value", "decimal(14,2)"), ("last_completed_order_ts", "timestamp"),
                  ("last_activity_ts", "timestamp")],
         select_statement=(
             f"WITH metrics AS (SELECT customer_id, "
             f"count(distinct case when status = 'completed' then order_id end) completed_orders, "
             f"count(distinct case when status = 'returned' then order_id end) returned_orders, "
             f"count(distinct case when status = 'pending' then order_id end) pending_orders, "
             f"count(distinct case when status = 'cancelled' then order_id end) cancelled_orders, "
             f"cast(coalesce(sum(case when status = 'completed' then net_amount else 0 end), 0) as decimal(14,2)) lifetime_value, "
             f"max(case when status = 'completed' then order_ts end) last_completed_order_ts, "
             f"max(order_ts) last_activity_ts "
             f"FROM {CATALOG}.{SILVER}.silver_sales_enriched GROUP BY customer_id) "
             f"SELECT c.customer_id, c.first_name, c.last_name, c.email, c.country, c.signup_date, "
             f"coalesce(m.completed_orders, 0) completed_orders, coalesce(m.returned_orders, 0) returned_orders, "
             f"coalesce(m.pending_orders, 0) pending_orders, coalesce(m.cancelled_orders, 0) cancelled_orders, "
             f"coalesce(m.lifetime_value, 0) lifetime_value, m.last_completed_order_ts, m.last_activity_ts "
             f"FROM {CATALOG}.{SILVER}.silver_customers c "
             f"LEFT JOIN metrics m ON c.customer_id = m.customer_id")),
]

FLOWS = [
    ("ds_medallion_bronze", BRONZE_MODELS, [BRONZE]),
    ("ds_medallion_silver", SILVER_MODELS, [SILVER]),
    ("ds_medallion_gold", GOLD_MODELS, [GOLD]),
]


# =============================================================================
#  helpers
# =============================================================================
def cpd_token() -> str:
    host = os.environ["WXD_CPD_HOST"]
    r = requests.post(f"https://{host}/icp4d-api/v1/authorize",
                      json={"username": os.getenv("WXD_CPD_USERNAME", "cpadmin"),
                            "api_key": os.environ["WXD_API_KEY"]}, verify=False)
    r.raise_for_status()
    return r.json()["token"]


def presto_conn():
    import prestodb
    user = os.environ["WXD_USER"]
    c = prestodb.dbapi.connect(
        host=os.environ["WXD_HOST"], port=int(os.getenv("WXD_PORT", "443")),
        user=user, catalog=CATALOG, http_scheme="https",
        auth=prestodb.auth.BasicAuthentication(user, os.environ["WXD_API_KEY"]),
        http_headers={"LhInstanceId": os.environ["WXD_INSTANCE_ID"]})
    c._http_session.verify = os.getenv("WXD_SSL_VERIFY", "certs/watsonxdata-ca.pem")
    return c


def build_docs() -> dict:
    docs = {}
    for name, models, _ in FLOWS:
        fb = FlowBuilder(PRESTO_CONNECTION, PROJECT_ID)
        for m in models:
            fb.add_model(**m)
        docs[name] = fb.render()
    return docs


def write_docs(docs: dict):
    FLOWS_DIR.mkdir(parents=True, exist_ok=True)
    for name, doc in docs.items():
        p = FLOWS_DIR / f"{name}.json"
        p.write_text(json.dumps(doc, indent=2))
        print(f"  wrote {p.relative_to(ROOT)}  ({len(doc['pipelines'][0]['nodes'])} nodes)")


def ensure_schemas(cur):
    for s in (BRONZE, SILVER, GOLD):
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
        print(f"  schema ready: {CATALOG}.{s}")


def create_flows(token: str, docs: dict):
    host = os.environ["WXD_CPD_HOST"]
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    for name, doc in docs.items():
        # delete existing same-named flow (idempotent) via the v2 assets API
        s = requests.post(f"https://{host}/v2/asset_types/data_intg_flow/search?project_id={PROJECT_ID}",
                          headers=h, json={"query": f'asset.name:"{name}"', "limit": 10}, verify=False).json()
        for r in s.get("results", []):
            aid = r["metadata"]["asset_id"]
            requests.delete(f"https://{host}/v2/assets/{aid}?project_id={PROJECT_ID}", headers=h, verify=False)
            print(f"  replaced existing {name} ({aid})")
        r = requests.post(
            f"https://{host}/data_intg/v3/data_intg_flows?project_id={PROJECT_ID}&data_intg_flow_name={name}",
            headers=h, data=json.dumps({"pipeline_flows": doc}), verify=False)
        if r.status_code in (200, 201):
            aid = r.json()["metadata"]["asset_id"]
            print(f"  CREATED {name}  ->  {aid}")
        else:
            print(f"  FAILED  {name}  -> {r.status_code} {r.text[:300]}")


def verify_parity(cur):
    """Run each model's SQL re-pointed at the populated dbt source tables and
    diff row counts (and gold value sums) against the dbt-built tables."""
    def to_dbt(sql):
        return sql.replace(BRONZE, "dbt_demo_bronze").replace(SILVER, "dbt_demo_silver")

    def scalar(sql):
        cur.execute(sql)
        return cur.fetchone()

    print("\n  model                         ds_rows  dbt_rows  match")
    print("  " + "-" * 56)
    all_ok = True
    for _, models, _ in FLOWS:
        for m in models:
            dbt_table = f"{CATALOG}.dbt_demo_{m['target_schema'].split('_demo_')[1]}.{m['target_table']}"
            ds_rows = scalar(f"SELECT count(*) FROM ({to_dbt(m['select_statement'])})")[0]
            dbt_rows = scalar(f"SELECT count(*) FROM {dbt_table}")[0]
            ok = ds_rows == dbt_rows
            all_ok &= ok
            print(f"  {m['target_table']:<28} {ds_rows:>8} {dbt_rows:>9}  {'OK' if ok else 'MISMATCH'}")
    # value-level checks on gold
    print("\n  gold value reconciliation (sum of measures):")
    checks = [
        ("gold_daily_sales.net_revenue",
         f"SELECT cast(sum(net_revenue) as decimal(18,2)) FROM ({to_dbt(GOLD_MODELS[0]['select_statement'])})",
         f"SELECT cast(sum(net_revenue) as decimal(18,2)) FROM {CATALOG}.dbt_demo_gold.gold_daily_sales"),
        ("gold_category_performance.total_revenue",
         f"SELECT cast(sum(total_revenue) as decimal(18,2)) FROM ({to_dbt(GOLD_MODELS[1]['select_statement'])})",
         f"SELECT cast(sum(total_revenue) as decimal(18,2)) FROM {CATALOG}.dbt_demo_gold.gold_category_performance"),
        ("gold_customer_360.lifetime_value",
         f"SELECT cast(sum(lifetime_value) as decimal(18,2)) FROM ({to_dbt(GOLD_MODELS[2]['select_statement'])})",
         f"SELECT cast(sum(lifetime_value) as decimal(18,2)) FROM {CATALOG}.dbt_demo_gold.gold_customer_360"),
    ]
    for label, ds_sql, dbt_sql in checks:
        dsv = scalar(ds_sql)[0]
        dbtv = scalar(dbt_sql)[0]
        ok = dsv == dbtv
        all_ok &= ok
        print(f"  {label:<42} ds={dsv}  dbt={dbtv}  {'OK' if ok else 'MISMATCH'}")
    print("\n  PARITY:", "ALL MATCH ✓" if all_ok else "DIFFERENCES FOUND ✗")
    return all_ok


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build", action="store_true", help="build + write the 3 flow JSON files")
    ap.add_argument("--create", action="store_true", help="build + create schemas + POST flows to CPD")
    ap.add_argument("--verify", action="store_true", help="Presto parity check vs the dbt tables")
    args = ap.parse_args()
    if not (args.build or args.create or args.verify):
        ap.error("pass at least one of --build / --create / --verify")

    docs = build_docs()
    if args.build or args.create:
        print("Building pipeline-flow JSON:")
        write_docs(docs)
    if args.create:
        print("\nCreating schemas + flows in CPD:")
        c = presto_conn(); ensure_schemas(c.cursor())
        create_flows(cpd_token(), docs)
    if args.verify:
        print("\nVerifying transformation parity against the dbt tables:")
        c = presto_conn()
        verify_parity(c.cursor())


if __name__ == "__main__":
    main()
