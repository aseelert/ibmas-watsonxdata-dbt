#!/usr/bin/env python3
"""
build_dashboard.py — assemble the watsonx.data dbt asset dashboard.

ALL data in this file was sourced exclusively via the IBM watsonx.data
intelligence MCP server (no direct Presto / dbt-artifact reads):
  - list_containers / search_asset      -> asset inventory + IDs
  - get_data_quality_for_asset          -> table-level DQ + dimensions
  - get_asset_details                   -> record counts, columns, per-column
                                           quality, data classes, key analysis
The dedicated lineage graph service (search_lineage_assets /
convert_asset_to_lineage_id / get_lineage_graph) returned HTTP 404 in this
environment, so the medallion lineage is reconstructed from MCP metadata
(layer tags + resource-key schema paths + FK key-analysis).

Emits: data.json (the dataset) and dashboard.html (self-contained, offline).
"""
import json
from pathlib import Path

PROJECT = "2d2415ea-71b5-4215-a7b6-b32a4889611e"
PROJECT_NAME = "ibmas-ingest-demo"
BASE_URL = "https://cpd-cpd-instance.apps.watson.ibmas-zocp-techcluster.org"

# ---- layer definitions (order = pipeline order) -----------------------------
LAYERS = {
    "ingest": {"label": "Landing / Ingest", "schema": "lakehouse_demo_ingest", "color": "#7c5cff"},
    "bronze": {"label": "Bronze (raw + lineage)", "schema": "lakehouse_demo_bronze", "color": "#cd7f32"},
    "silver": {"label": "Silver (clean / conformed)", "schema": "lakehouse_demo_silver", "color": "#aab2bd"},
    "gold":   {"label": "Gold (business marts)", "schema": "lakehouse_demo_gold", "color": "#ffd34d"},
}

# ---- table-level data quality (get_data_quality_for_asset) -------------------
DQ = {
 "customers":               {"overall":100.00,"validity":100.00,"consistency":100.0,"uniqueness":100.0,"completeness":100.0},
 "orders":                  {"overall":96.92, "validity":87.69, "consistency":100.0,"uniqueness":100.0,"completeness":100.0},
 "products":                {"overall":95.83, "validity":87.50, "consistency":None, "uniqueness":100.0,"completeness":100.0},
 "order_items":             {"overall":95.79, "validity":87.37, "consistency":None, "uniqueness":100.0,"completeness":100.0},
 "bronze_customers":        {"overall":100.00,"validity":100.00,"consistency":100.0,"uniqueness":100.0,"completeness":100.0},
 "bronze_orders":           {"overall":98.55, "validity":94.20, "consistency":100.0,"uniqueness":100.0,"completeness":100.0},
 "bronze_products":         {"overall":99.38, "validity":97.50, "consistency":100.0,"uniqueness":100.0,"completeness":100.0},
 "bronze_order_items":      {"overall":98.65, "validity":94.59, "consistency":100.0,"uniqueness":100.0,"completeness":100.0},
 "silver_customers":        {"overall":100.00,"validity":100.00,"consistency":100.0,"uniqueness":100.0,"completeness":100.0},
 "silver_orders":           {"overall":98.14, "validity":92.54, "consistency":100.0,"uniqueness":100.0,"completeness":100.0},
 "silver_products":         {"overall":98.89, "validity":96.67, "consistency":None, "uniqueness":100.0,"completeness":100.0},
 "silver_order_items":      {"overall":99.44, "validity":98.32, "consistency":None, "uniqueness":100.0,"completeness":100.0},
 "silver_sales_enriched":   {"overall":97.40, "validity":89.60, "consistency":100.0,"uniqueness":100.0,"completeness":100.0},
 "gold_customer_360":       {"overall":98.59, "validity":94.36, "consistency":100.0,"uniqueness":100.0,"completeness":100.0},
 "gold_category_performance":{"overall":98.33,"validity":95.00, "consistency":None, "uniqueness":100.0,"completeness":100.0},
 "gold_daily_sales":        {"overall":94.05, "validity":88.11, "consistency":None, "uniqueness":None, "completeness":100.0},
 "time_spine_daily":        {"overall":100.00,"validity":100.00,"consistency":None, "uniqueness":100.0,"completeness":100.0},
}

# ---- per-asset detail (get_asset_details): record count, key analysis, cols --
DETAILS = {
"products": {"id":"fe74fed3-7470-463c-a35d-d9c9bad7183c","layer":"ingest","records":8,"pk":2,"fk":1,
  "cols":[["product_id","INTEGER",100,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["product_name","varchar",100,"Organization Name",["Uniqueness","Completeness"]],
          ["category","varchar",100,None,["Completeness"]],
          ["unit_price","DOUBLE",91.67,"Identifier",["Validity","Uniqueness","Completeness"]]]},
"orders": {"id":"6a95b469-9c42-4078-aa37-378ac94e82fa","layer":"ingest","records":13,"pk":1,"fk":4,
  "cols":[["order_id","INTEGER",100,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["customer_id","INTEGER",100,"Customer Number",["Validity","Completeness"]],
          ["order_ts","TIMESTAMP",100,"Date",["Validity","Uniqueness","Completeness"]],
          ["status","varchar",92.31,"Organization Name",["Validity","Completeness","Consistency"]],
          ["payment_method","varchar",87.18,"Code",["Validity","Completeness","Consistency"]]]},
"customers": {"id":"edc3cf16-7d69-41d6-82bf-4230aaa00b34","layer":"ingest","records":8,"pk":4,"fk":0,
  "cols":[["customer_id","INTEGER",100,"Customer Number",["Validity","Uniqueness","Completeness"]],
          ["first_name","varchar",100,None,["Uniqueness","Completeness"]],
          ["last_name","varchar",100,"Last Name",["Uniqueness","Completeness"]],
          ["email","varchar",100,"Email Address",["Uniqueness","Completeness","Consistency","Validity"]],
          ["signup_date","DATE",100,"Date",["Validity","Uniqueness","Completeness"]],
          ["country","varchar",100,"Country Code",["Validity","Completeness","Consistency"]]]},
"order_items": {"id":"9ae57cb0-0123-407c-bf0d-d2e6fd9f5a5b","layer":"ingest","records":19,"pk":1,"fk":6,
  "cols":[["order_item_id","INTEGER",84.21,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["order_id","INTEGER",100,None,["Validity","Completeness"]],
          ["product_id","INTEGER",100,None,["Validity","Completeness"]],
          ["quantity","INTEGER",100,"Code",["Validity","Completeness"]],
          ["discount_pct","DOUBLE",92.11,None,["Validity","Completeness"]]]},
"bronze_products": {"id":"fc561f29-617c-4704-8709-36449c7ae73c","layer":"bronze","records":20,"pk":1,"fk":2,
  "cols":[["product_id","INTEGER",100,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["product_name","varchar",100,"Text",["Uniqueness","Completeness"]],
          ["category","varchar",100,"Code",["Completeness"]],
          ["unit_price","DOUBLE",95,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["_ingested_at","TIMESTAMP WITH TIME ZONE",100,"Date",["Validity","Completeness"]],
          ["_ingested_by","varchar",100,"Indicator",["Validity","Completeness","Consistency"]],
          ["_source_file","varchar",100,None,["Validity","Completeness","Consistency"]],
          ["_ingest_batch_id","varchar",100,"Indicator",["Validity","Completeness","Consistency"]]]},
"bronze_orders": {"id":"eb10fdc0-d5e6-44ca-ab53-4a9ae3f82fcc","layer":"bronze","records":500,"pk":1,"fk":4,
  "cols":[["order_id","INTEGER",100,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["customer_id","INTEGER",100,"Customer Number",["Validity","Completeness"]],
          ["order_ts","TIMESTAMP",100,"Date",["Validity","Uniqueness","Completeness"]],
          ["status","varchar",94.2,"Organization Name",["Validity","Completeness","Consistency"]],
          ["payment_method","varchar",88.4,"Code",["Validity","Completeness","Consistency"]],
          ["_ingested_at","TIMESTAMP WITH TIME ZONE",100,"Date",["Validity","Completeness"]],
          ["_ingested_by","varchar",100,"Indicator",["Validity","Completeness","Consistency"]],
          ["_source_file","varchar",100,"Indicator",["Validity","Completeness","Consistency"]],
          ["_ingest_batch_id","varchar",100,"Indicator",["Validity","Completeness","Consistency"]]]},
"bronze_customers": {"id":"bd376e17-e422-460d-9f19-0f94c9df29cb","layer":"bronze","records":50,"pk":4,"fk":8,
  "cols":[["customer_id","INTEGER",100,"Customer Number",["Validity","Uniqueness","Completeness"]],
          ["first_name","varchar",100,"First Name",["Uniqueness","Completeness"]],
          ["last_name","varchar",100,"Last Name",["Uniqueness","Completeness"]],
          ["email","varchar",100,"Email Address",["Uniqueness","Completeness","Consistency","Validity"]],
          ["signup_date","DATE",100,"Date",["Validity","Completeness"]],
          ["country","varchar",100,"Country Code",["Validity","Completeness","Consistency"]],
          ["_ingested_at","TIMESTAMP WITH TIME ZONE",100,"Date",["Validity","Completeness"]],
          ["_ingested_by","varchar",100,"Indicator",["Validity","Completeness","Consistency"]],
          ["_source_file","varchar",100,None,["Validity","Completeness","Consistency"]],
          ["_ingest_batch_id","varchar",100,"Indicator",["Validity","Completeness","Consistency"]]]},
"bronze_order_items": {"id":"3f5a2a23-0846-42e5-a126-f998146ea953","layer":"bronze","records":None,"pk":1,"fk":6,
  "cols":[["order_item_id","INTEGER",96.67,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["order_id","INTEGER",100,None,["Validity","Completeness"]],
          ["product_id","INTEGER",100,"Code",["Validity","Completeness"]],
          ["quantity","INTEGER",100,"Code",["Validity","Completeness"]],
          ["discount_pct","DOUBLE",80.65,None,["Validity","Completeness"]],
          ["_ingested_at","TIMESTAMP WITH TIME ZONE",100,"Date",["Validity","Completeness"]],
          ["_ingested_by","varchar",100,"Indicator",["Validity","Completeness","Consistency"]],
          ["_source_file","varchar",100,None,["Validity","Completeness","Consistency"]],
          ["_ingest_batch_id","varchar",100,"Indicator",["Validity","Completeness","Consistency"]]]},
"silver_customers": {"id":"ed9bd4af-b082-4d56-90e8-38bbe13d7809","layer":"silver","records":50,"pk":4,"fk":8,
  "cols":[["customer_id","INTEGER",100,"Customer Number",["Validity","Uniqueness","Completeness"]],
          ["first_name","varchar",100,"First Name",["Uniqueness","Completeness"]],
          ["last_name","varchar",100,"Last Name",["Uniqueness","Completeness"]],
          ["email","varchar",100,"Email Address",["Uniqueness","Completeness","Consistency","Validity"]],
          ["signup_date","DATE",100,"Date",["Validity","Completeness"]],
          ["country","varchar",100,"Country Code",["Validity","Completeness","Consistency"]],
          ["transformed_at","TIMESTAMP WITH TIME ZONE",100,"Date",["Validity","Completeness"]]]},
"silver_orders": {"id":"e87bc6c3-b68d-4956-99a1-ce3555b1d862","layer":"silver","records":500,"pk":1,"fk":4,
  "cols":[["order_id","INTEGER",100,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["customer_id","INTEGER",100,"Customer Number",["Validity","Completeness"]],
          ["order_ts","TIMESTAMP",100,"Date",["Validity","Uniqueness","Completeness"]],
          ["order_date","DATE",100,"Date",["Validity","Completeness"]],
          ["status","varchar",94.2,"Organization Name",["Validity","Completeness","Consistency"]],
          ["payment_method","varchar",88.4,"Code",["Validity","Completeness","Consistency"]],
          ["transformed_at","TIMESTAMP WITH TIME ZONE",100,"Date",["Validity","Completeness"]]]},
"silver_products": {"id":"62e4e29c-7f99-4859-aeaa-2ceb51eabf61","layer":"silver","records":20,"pk":1,"fk":2,
  "cols":[["product_id","INTEGER",100,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["product_name","varchar",100,"Text",["Uniqueness","Completeness"]],
          ["category","varchar",100,"Code",["Completeness"]],
          ["unit_price","DECIMAL(12,2)",96.67,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["transformed_at","TIMESTAMP WITH TIME ZONE",100,"Date",["Validity","Completeness"]]]},
"silver_order_items": {"id":"061fcea1-066d-4ca5-91eb-cee31fd1abb2","layer":"silver","records":None,"pk":1,"fk":5,
  "cols":[["order_item_id","INTEGER",96.63,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["order_id","INTEGER",100,None,["Validity","Completeness"]],
          ["product_id","INTEGER",100,"Code",["Validity","Completeness"]],
          ["quantity","INTEGER",100,"Code",["Validity","Completeness"]],
          ["discount_pct","DECIMAL(5,2)",100,None,["Validity","Completeness"]],
          ["transformed_at","TIMESTAMP WITH TIME ZONE",100,"Date",["Validity","Completeness"]]]},
"silver_sales_enriched": {"id":"ac4000e1-f258-4c61-8fb5-ed36777e6a43","layer":"silver","records":1134,"pk":1,"fk":7,
  "cols":[["order_item_id","INTEGER",93.2,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["order_id","INTEGER",100,None,["Validity","Completeness"]],
          ["order_date","DATE",100,"Date",["Validity","Completeness"]],
          ["order_ts","TIMESTAMP",100,"Date",["Validity","Completeness"]],
          ["status","varchar",94.23,"Organization Name",["Validity","Completeness","Consistency"]],
          ["payment_method","varchar",88.3,"Code",["Validity","Completeness","Consistency"]],
          ["customer_id","INTEGER",100,"Customer Number",["Validity","Completeness"]],
          ["customer_country","varchar",100,"Country Code",["Validity","Completeness","Consistency"]],
          ["product_id","INTEGER",100,"Code",["Validity","Completeness"]],
          ["product_name","varchar",100,"Text",["Completeness"]],
          ["category","varchar",100,"Code",["Completeness"]],
          ["quantity","INTEGER",100,"Code",["Validity","Completeness"]],
          ["unit_price","DECIMAL(12,2)",94.35,None,["Validity","Completeness"]],
          ["discount_pct","DECIMAL(5,2)",100,None,["Validity","Completeness"]],
          ["gross_amount","DECIMAL(14,2)",81.6,None,["Validity","Completeness"]],
          ["net_amount","DECIMAL(14,2)",82.45,None,["Validity","Completeness"]],
          ["transformed_at","TIMESTAMP WITH TIME ZONE",100,"Date",["Validity","Completeness"]]]},
"time_spine_daily": {"id":"e2093ad4-9f2f-476b-8b31-712c7dcc0639","layer":"silver","records":365,"pk":None,"fk":None,
  "cols":[["date_day","DATE",100,"Date",["Validity","Uniqueness","Completeness"]]]},
"gold_customer_360": {"id":"67193b4c-f930-4759-b4e2-d0b67b9671d3","layer":"gold","records":50,"pk":4,"fk":8,
  "cols":[["customer_id","INTEGER",100,"Customer Number",["Validity","Uniqueness","Completeness"]],
          ["first_name","varchar",100,"First Name",["Uniqueness","Completeness"]],
          ["last_name","varchar",100,"Last Name",["Uniqueness","Completeness"]],
          ["email","varchar",100,"Email Address",["Uniqueness","Completeness","Consistency","Validity"]],
          ["country","varchar",100,"Country Code",["Validity","Completeness","Consistency"]],
          ["signup_date","DATE",100,"Date",["Validity","Completeness"]],
          ["completed_orders","BIGINT",89,"Code",["Validity","Completeness"]],
          ["returned_orders","BIGINT",100,"Code",["Validity","Completeness"]],
          ["pending_orders","BIGINT",99,"Boolean",["Validity","Completeness"]],
          ["cancelled_orders","BIGINT",93,"Boolean",["Validity","Completeness"]],
          ["lifetime_value","DECIMAL(14,2)",92,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["last_completed_order_ts","TIMESTAMP",100,"Date",["Validity","Uniqueness","Completeness"]],
          ["last_activity_ts","TIMESTAMP",100,"Date",["Validity","Uniqueness","Completeness"]]]},
"gold_category_performance": {"id":"5183bd38-5204-47a4-834f-604855f668b0","layer":"gold","records":5,"pk":3,"fk":0,
  "cols":[["category","varchar",100,None,["Uniqueness","Completeness"]],
          ["total_orders","BIGINT",100,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["total_units","BIGINT",100,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["total_revenue","DECIMAL(14,2)",93.33,"Identifier",["Validity","Uniqueness","Completeness"]],
          ["avg_revenue_per_unit","DECIMAL(14,2)",100,"Identifier",["Validity","Uniqueness","Completeness"]]]},
"gold_daily_sales": {"id":"ae46b938-b082-49c7-a945-853e16df3151","layer":"gold","records":494,"pk":0,"fk":1,
  "cols":[["order_date","DATE",100,"Date",["Validity","Completeness"]],
          ["category","varchar",100,"Code",["Completeness"]],
          ["order_count","BIGINT",100,"Code",["Validity","Completeness"]],
          ["units_sold","BIGINT",98.38,"Code",["Validity","Completeness"]],
          ["net_revenue","DECIMAL(14,2)",77.83,"Quantity",["Validity","Completeness"]]]},
}

# ---- lineage edges (medallion flow, reconstructed from MCP layer metadata) ---
EDGES = [
  ("products","bronze_products"),("orders","bronze_orders"),
  ("customers","bronze_customers"),("order_items","bronze_order_items"),
  ("bronze_products","silver_products"),("bronze_orders","silver_orders"),
  ("bronze_customers","silver_customers"),("bronze_order_items","silver_order_items"),
  ("silver_order_items","silver_sales_enriched"),("silver_orders","silver_sales_enriched"),
  ("silver_products","silver_sales_enriched"),("silver_customers","silver_sales_enriched"),
  ("silver_sales_enriched","gold_daily_sales"),("silver_sales_enriched","gold_customer_360"),
  ("silver_customers","gold_customer_360"),("gold_daily_sales","gold_category_performance"),
]

# ---- key relationships (FK, from data classes + key-analysis FK suggestions) -
RELATIONSHIPS = [
  ("orders","customer_id","customers","customer_id","Customer Number"),
  ("order_items","order_id","orders","order_id","Identifier"),
  ("order_items","product_id","products","product_id","Identifier"),
  ("bronze_orders","customer_id","bronze_customers","customer_id","Customer Number"),
  ("silver_orders","customer_id","silver_customers","customer_id","Customer Number"),
  ("silver_order_items","order_id","silver_orders","order_id","Identifier"),
  ("silver_order_items","product_id","silver_products","product_id","Code"),
  ("silver_sales_enriched","customer_id","silver_customers","customer_id","Customer Number"),
  ("silver_sales_enriched","product_id","silver_products","product_id","Code"),
  ("silver_sales_enriched","order_id","silver_orders","order_id","Identifier"),
  ("gold_customer_360","customer_id","silver_customers","customer_id","Customer Number"),
]

def build_assets():
    assets = []
    for name, d in DETAILS.items():
        cols = [{"name":c[0],"type":c[1],"quality":c[2],"data_class":c[3],"checks":c[4]} for c in d["cols"]]
        scored = [c["quality"] for c in cols if c["quality"] is not None]
        assets.append({
            "name": name, "id": d["id"], "layer": d["layer"],
            "schema": LAYERS[d["layer"]]["schema"],
            "records": d["records"],
            "pk_suggested": d["pk"], "fk_suggested": d["fk"],
            "n_cols": len(cols),
            "n_classified": sum(1 for c in cols if c["data_class"]),
            "dq": DQ[name],
            "col_avg": round(sum(scored)/len(scored),1) if scored else None,
            "columns": cols,
            "url": f"{BASE_URL}/projects/{PROJECT}/data-assets/{d['id']}?context=cpd",
        })
    return assets

def main():
    out_dir = Path(__file__).parent
    assets = build_assets()
    # data-class catalog
    classes = {}
    for a in assets:
        for c in a["columns"]:
            if c["data_class"]:
                classes.setdefault(c["data_class"], []).append(f'{a["name"]}.{c["name"]}')
    # totals
    total_records = sum(a["records"] or 0 for a in assets)
    avg_dq = round(sum(a["dq"]["overall"] for a in assets)/len(assets),2)
    data = {
        "meta": {
            "project": PROJECT_NAME, "project_id": PROJECT,
            "base_url": BASE_URL,
            "source": "IBM watsonx.data intelligence MCP server only",
            "lineage_service": "unavailable (HTTP 404) — medallion lineage reconstructed from MCP layer metadata",
            "business_terms": "none assigned to assets (get_asset_glossary_artifacts returned none); governance present via auto-discovered data classes",
            "generated": "2026-06-25",
        },
        "layers": LAYERS,
        "kpis": {
            "n_assets": len(assets), "avg_dq": avg_dq, "total_records": total_records,
            "n_classes": len(classes),
            "n_columns": sum(a["n_cols"] for a in assets),
        },
        "assets": assets, "edges": EDGES, "relationships": RELATIONSHIPS,
        "data_classes": classes,
    }
    (out_dir/"data.json").write_text(json.dumps(data, indent=2))
    html = HTML_TEMPLATE.replace("/*DATA*/", json.dumps(data))
    (out_dir/"dashboard.html").write_text(html)
    print(f"Wrote data.json ({len(assets)} assets) and dashboard.html")
    print(f"  avg DQ={avg_dq}  total records={total_records:,}  data classes={len(classes)}")

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>watsonx.data · dbt Medallion — DQ &amp; Lineage Dashboard</title>
<style>
:root{--bg:#0d1117;--panel:#161b22;--panel2:#1c2330;--bd:#2a3340;--tx:#e6edf3;--mut:#8b949e;
--bronze:#cd7f32;--silver:#aab2bd;--gold:#ffd34d;--ingest:#7c5cff;--good:#3fb950;--warn:#d29922;--bad:#f85149;--accent:#58a6ff;}
*{box-sizing:border-box}
body{margin:0;background:linear-gradient(180deg,#0b1f3a 0%,var(--bg) 40%);color:var(--tx);font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
.wrap{max-width:1320px;margin:0 auto;padding:28px 22px 80px}
h1{font-size:24px;margin:0 0 4px;letter-spacing:-.3px}
h2{font-size:16px;text-transform:uppercase;letter-spacing:1px;color:var(--mut);margin:34px 0 14px;font-weight:600}
.sub{color:var(--mut);font-size:13px}
.src{display:inline-block;margin-top:8px;padding:5px 11px;border:1px solid var(--bd);border-radius:20px;background:var(--panel);font-size:12px;color:var(--accent)}
.kpis{display:grid;grid-template-columns:repeat(5,1fr);gap:14px;margin-top:22px}
.kpi{background:var(--panel);border:1px solid var(--bd);border-radius:12px;padding:16px 18px}
.kpi .v{font-size:30px;font-weight:700;letter-spacing:-1px}
.kpi .l{color:var(--mut);font-size:12px;text-transform:uppercase;letter-spacing:.6px;margin-top:2px}
.kpi.dq .v{color:var(--good)}
/* pipeline */
.pipe{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}
.lay{background:var(--panel);border:1px solid var(--bd);border-top:3px solid;border-radius:12px;padding:14px}
.lay h3{margin:0 0 3px;font-size:15px;display:flex;align-items:center;gap:8px}
.lay .meta{color:var(--mut);font-size:11px;font-family:ui-monospace,monospace;margin-bottom:10px}
.node{background:var(--panel2);border:1px solid var(--bd);border-radius:8px;padding:9px 11px;margin-bottom:8px;cursor:pointer;transition:.15s}
.node:hover{border-color:var(--accent);transform:translateX(2px)}
.node .nm{font-weight:600;font-size:13px;display:flex;justify-content:space-between;align-items:center;gap:6px}
.node .ln{display:flex;gap:10px;color:var(--mut);font-size:11px;margin-top:5px;font-family:ui-monospace,monospace}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block}
.badge{font-size:10px;font-weight:700;padding:2px 7px;border-radius:10px}
/* lineage svg */
.lin{background:var(--panel);border:1px solid var(--bd);border-radius:12px;padding:6px;overflow-x:auto}
svg text{fill:var(--tx);font:11px ui-monospace,monospace}
.gnode rect{rx:6;cursor:pointer}
.gnode:hover rect{stroke:var(--accent);stroke-width:2}
/* table */
table{width:100%;border-collapse:collapse;background:var(--panel);border:1px solid var(--bd);border-radius:12px;overflow:hidden}
th,td{padding:9px 11px;text-align:left;border-bottom:1px solid var(--bd);font-size:13px}
th{color:var(--mut);text-transform:uppercase;font-size:11px;letter-spacing:.6px;cursor:pointer;user-select:none;white-space:nowrap}
th:hover{color:var(--tx)}
td.num,th.num{text-align:right;font-family:ui-monospace,monospace}
tr.arow{cursor:pointer}
tr.arow:hover{background:var(--panel2)}
.lpill{font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;text-transform:uppercase}
.bar{position:relative;height:18px;background:var(--panel2);border-radius:4px;min-width:120px;overflow:hidden}
.bar>span{position:absolute;left:0;top:0;bottom:0;border-radius:4px}
.bar>b{position:absolute;right:5px;top:0;bottom:0;display:flex;align-items:center;font-size:11px;font-weight:600;font-family:ui-monospace,monospace}
.detail{background:var(--panel2)}
.detail td{padding:0}
.cwrap{padding:14px 18px}
.ctab{width:100%;background:transparent;border:0}
.ctab th{background:transparent}
.ctab td,.ctab th{border-bottom:1px solid var(--bd);padding:6px 8px}
.chip{display:inline-block;font-size:10px;padding:1px 7px;border-radius:9px;background:#21324a;color:#9ecbff;margin:1px 2px;border:1px solid #2c4156}
.chk{font-size:10px;padding:1px 6px;border-radius:8px;margin:1px;display:inline-block;border:1px solid var(--bd);color:var(--mut)}
.miniq{font-family:ui-monospace,monospace;font-weight:700}
.legend{display:flex;gap:16px;flex-wrap:wrap;margin:10px 0 0;color:var(--mut);font-size:12px}
.legend span{display:flex;align-items:center;gap:6px}
.classgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:10px}
.cls{background:var(--panel);border:1px solid var(--bd);border-radius:10px;padding:11px 13px}
.cls .h{font-weight:700;display:flex;justify-content:space-between}
.cls .cols{color:var(--mut);font-size:11px;margin-top:5px;font-family:ui-monospace,monospace;line-height:1.5}
.rel td:nth-child(2),.rel td:nth-child(4){font-family:ui-monospace,monospace;color:var(--accent)}
.note{background:var(--panel);border:1px solid var(--bd);border-left:3px solid var(--warn);border-radius:8px;padding:12px 15px;color:var(--mut);font-size:13px;margin-top:12px}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
.foot{margin-top:40px;color:var(--mut);font-size:12px;border-top:1px solid var(--bd);padding-top:16px}
</style></head>
<body><div class="wrap">
<h1>watsonx.data · dbt Medallion — Data Quality &amp; Lineage</h1>
<div class="sub">Project <b id="pj"></b> · every metric sourced live from the <b>IBM watsonx.data intelligence MCP server</b></div>
<span class="src" id="srcline"></span>

<div class="kpis" id="kpis"></div>

<h2>Medallion pipeline — layers &amp; assets</h2>
<div class="legend">
 <span><i class="dot" style="background:var(--ingest)"></i>Ingest / Landing</span>
 <span><i class="dot" style="background:var(--bronze)"></i>Bronze</span>
 <span><i class="dot" style="background:var(--silver)"></i>Silver</span>
 <span><i class="dot" style="background:var(--gold)"></i>Gold</span>
 <span>● DQ ≥99 good · 95–99 ok · &lt;95 watch</span>
</div>
<div class="pipe" id="pipe" style="margin-top:12px"></div>

<h2>Lineage &amp; data flow</h2>
<div class="lin"><div id="lineage"></div></div>
<div class="note" id="linnote"></div>

<h2>Asset register — DQ, records, structure <span class="sub" style="text-transform:none;font-weight:400">(click a row for column-level detail)</span></h2>
<table id="atable"><thead><tr>
<th data-k="name">Asset</th><th data-k="layer">Layer</th>
<th class="num" data-k="records">Records</th>
<th data-k="overall">Overall DQ</th>
<th class="num" data-k="validity">Valid</th><th class="num" data-k="completeness">Compl</th>
<th class="num" data-k="uniqueness">Uniq</th><th class="num" data-k="consistency">Consist</th>
<th class="num" data-k="n_cols">Cols</th><th class="num" data-k="n_classified">Classified</th>
<th class="num" data-k="fk_suggested">FK*</th>
</tr></thead><tbody id="abody"></tbody></table>
<div class="sub" style="margin-top:6px">FK* / PK = key-analysis suggestions from MCP profiling. “Classified” = columns with an auto-discovered data class.</div>

<h2>Key relationships (foreign keys)</h2>
<table class="rel"><thead><tr><th>From asset</th><th>Column</th><th>References</th><th>Column</th><th>Data class</th></tr></thead>
<tbody id="relbody"></tbody></table>

<h2>Governance — auto-discovered data classes</h2>
<div class="sub" style="margin:-6px 0 12px">Business <i>terms</i> are not yet assigned to these assets; governance is carried by data <i>classes</i> the MCP profiler detected (incl. PII: Email Address, names, Customer Number).</div>
<div class="classgrid" id="classes"></div>

<div class="foot" id="foot"></div>
</div>
<script>
const D = /*DATA*/;
const LC = D.layers, order=["ingest","bronze","silver","gold"];
const cvar=l=>getComputedStyle(document.documentElement).getPropertyValue('--'+l);
const qcolor=q=> q==null?'#30363d': q>=99?'#3fb950':q>=95?'#58a6ff':q>=90?'#d29922':'#f85149';
const fmt=n=> n==null?'—':n.toLocaleString();
const byName=Object.fromEntries(D.assets.map(a=>[a.name,a]));

document.getElementById('pj').textContent=D.meta.project;
document.getElementById('srcline').textContent='Source: '+D.meta.source+'  ·  generated '+D.meta.generated;

// KPIs
const k=D.kpis;
document.getElementById('kpis').innerHTML=[
 ['n',k.n_assets,'dbt assets'],
 ['dq',k.avg_dq+'%','avg data quality'],
 ['n',fmt(k.total_records),'records profiled'],
 ['n',k.n_columns,'columns'],
 ['n',k.n_classes,'data classes'],
].map(([c,v,l])=>`<div class="kpi ${c==='dq'?'dq':''}"><div class="v">${v}</div><div class="l">${l}</div></div>`).join('');

// pipeline columns
document.getElementById('pipe').innerHTML=order.map(L=>{
 const lay=LC[L], col=cvar(L);
 const nodes=D.assets.filter(a=>a.layer===L);
 return `<div class="lay" style="border-top-color:${col}">
  <h3><i class="dot" style="background:${col}"></i>${lay.label}</h3>
  <div class="meta">${lay.schema} · ${nodes.length} assets</div>
  ${nodes.map(a=>`<div class="node" onclick="jump('${a.name}')">
     <div class="nm">${a.name}<span class="badge" style="background:${qcolor(a.dq.overall)}22;color:${qcolor(a.dq.overall)}">${a.dq.overall}%</span></div>
     <div class="ln"><span>▦ ${fmt(a.records)} rows</span><span>▤ ${a.n_cols} cols</span></div>
   </div>`).join('')}
 </div>`;
}).join('');

// ---- lineage SVG (layered left->right) ----
(function(){
 const cols={ingest:[],bronze:[],silver:[],gold:[]};
 D.assets.forEach(a=>cols[a.layer].push(a.name));
 const colX={ingest:30,bronze:330,silver:630,gold:980};
 const W=160,H=34,vgap=14;
 const pos={};
 order.forEach(L=>{cols[L].forEach((n,i)=>{pos[n]={x:colX[L],y:30+i*(H+vgap),w:W,h:H,l:L};});});
 const maxRows=Math.max(...order.map(L=>cols[L].length));
 const height=30+maxRows*(H+vgap)+20, width=1180;
 let s=`<svg viewBox="0 0 ${width} ${height}" width="${width}" height="${height}">`;
 s+=`<defs><marker id="ar" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 z" fill="#5b6673"/></marker></defs>`;
 // edges
 D.edges.forEach(([a,b])=>{
   const p=pos[a],q=pos[b]; if(!p||!q)return;
   const x1=p.x+p.w,y1=p.y+p.h/2,x2=q.x,y2=q.y+q.h/2,mx=(x1+x2)/2;
   s+=`<path d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2-7},${y2}" fill="none" stroke="#3a4452" stroke-width="1.4" marker-end="url(#ar)"/>`;
 });
 // nodes
 order.forEach(L=>cols[L].forEach(n=>{
   const a=byName[n],p=pos[n],col=cvar(L);
   s+=`<g class="gnode" onclick="jump('${n}')">
     <rect x="${p.x}" y="${p.y}" width="${p.w}" height="${p.h}" fill="#1c2330" stroke="${col}" stroke-width="1.3"/>
     <rect x="${p.x}" y="${p.y}" width="4" height="${p.h}" fill="${col}"/>
     <circle cx="${p.x+p.w-13}" cy="${p.y+p.h/2}" r="5" fill="${qcolor(a.dq.overall)}"/>
     <text x="${p.x+11}" y="${p.y+15}" style="font-weight:700">${n.length>20?n.slice(0,19)+'…':n}</text>
     <text x="${p.x+11}" y="${p.y+27}" fill="#8b949e">${fmt(a.records)} rows · ${a.dq.overall}%</text>
   </g>`;
 }));
 s+=`</svg>`;
 document.getElementById('lineage').innerHTML=s;
})();
document.getElementById('linnote').innerHTML='⚠ '+D.meta.lineage_service+'. Arrows show the medallion data flow; coloured dot = overall DQ.';

// ---- asset table ----
let sortK='layer',sortDir=1;
const layRank={ingest:0,bronze:1,silver:2,gold:3};
function val(a,k){
 if(k==='layer')return layRank[a.layer];
 if(['overall','validity','completeness','uniqueness','consistency'].includes(k))return a.dq[k]??-1;
 if(['records','n_cols','n_classified','fk_suggested'].includes(k))return a[k]??-1;
 return a.name;
}
function renderTable(){
 const rows=[...D.assets].sort((x,y)=>{const a=val(x,sortK),b=val(y,sortK);return (a>b?1:a<b?-1:0)*sortDir;});
 const tb=document.getElementById('abody');tb.innerHTML='';
 rows.forEach(a=>{
  const col=cvar(a.layer);
  const dimcell=v=> v==null?'<td class="num">—</td>':`<td class="num" style="color:${qcolor(v)}">${v}</td>`;
  const tr=document.createElement('tr');tr.className='arow';
  tr.innerHTML=`<td><b>${a.name}</b><div class="sub" style="font-size:11px">${a.schema}</div></td>
   <td><span class="lpill" style="background:${col}22;color:${col}">${a.layer}</span></td>
   <td class="num">${fmt(a.records)}</td>
   <td><div class="bar"><span style="width:${a.dq.overall}%;background:${qcolor(a.dq.overall)}"></span><b>${a.dq.overall}%</b></div></td>
   ${dimcell(a.dq.validity)}${dimcell(a.dq.completeness)}${dimcell(a.dq.uniqueness)}${dimcell(a.dq.consistency)}
   <td class="num">${a.n_cols}</td><td class="num">${a.n_classified}/${a.n_cols}</td>
   <td class="num">${a.fk_suggested==null?'—':a.fk_suggested}</td>`;
  const det=document.createElement('tr');det.className='detail';det.style.display='none';
  det.innerHTML=`<td colspan="11"><div class="cwrap">${colTable(a)}</div></td>`;
  tr.onclick=()=>{det.style.display=det.style.display==='none'?'':'none';};
  tb.appendChild(tr);tb.appendChild(det);a._det=det;a._tr=tr;
 });
}
function colTable(a){
 return `<div style="margin-bottom:8px"><b>${a.name}</b> · column-level data quality &amp; classes ·
   <a href="${a.url}" target="_blank">open in watsonx ↗</a> · PK suggested: ${a.pk_suggested??'—'} · FK suggested: ${a.fk_suggested??'—'}</div>
 <table class="ctab"><thead><tr><th>Column</th><th>Type</th><th class="num">Quality</th><th>Data class</th><th>DQ dimensions checked</th></tr></thead><tbody>
 ${a.columns.map(c=>`<tr>
   <td><b>${c.name}</b></td><td class="sub">${c.type}</td>
   <td class="num miniq" style="color:${qcolor(c.quality)}">${c.quality==null?'—':c.quality}</td>
   <td>${c.data_class?`<span class="chip">${c.data_class}</span>`:'<span class="sub">—</span>'}</td>
   <td>${c.checks.map(k=>`<span class="chk">${k}</span>`).join('')||'<span class="sub">—</span>'}</td>
 </tr>`).join('')}
 </tbody></table>`;
}
document.querySelectorAll('#atable th').forEach(th=>th.onclick=()=>{
 const k=th.dataset.k; if(sortK===k)sortDir*=-1; else{sortK=k;sortDir=1;} renderTable();
});
renderTable();
function jump(name){
 const a=byName[name];document.getElementById('atable').scrollIntoView({behavior:'smooth',block:'center'});
 if(a._det.style.display==='none'){a._det.style.display='';}
 a._tr.style.outline='2px solid var(--accent)';setTimeout(()=>a._tr.style.outline='',1600);
}

// relationships
document.getElementById('relbody').innerHTML=D.relationships.map(r=>
 `<tr><td>${r[0]}</td><td>${r[1]}</td><td>${r[2]}</td><td>${r[3]}</td><td><span class="chip">${r[4]}</span></td></tr>`).join('');

// data classes
const cls=Object.entries(D.data_classes).sort((a,b)=>b[1].length-a[1].length);
document.getElementById('classes').innerHTML=cls.map(([name,cols])=>{
 const pii=/Email|Name|Customer Number/.test(name);
 return `<div class="cls"><div class="h"><span>${name}${pii?' 🔒':''}</span><span class="sub">${cols.length}</span></div>
  <div class="cols">${cols.slice(0,8).join('<br>')}${cols.length>8?'<br>+'+(cols.length-8)+' more':''}</div></div>`;
}).join('');

document.getElementById('foot').innerHTML=
 `Generated ${D.meta.generated} · ${D.kpis.n_assets} assets · all data via watsonx.data intelligence MCP (get_data_quality_for_asset, get_asset_details, search_asset, list_containers).<br>`+
 `Lineage service: ${D.meta.lineage_service}.<br>Business terms: ${D.meta.business_terms}.`;
</script></body></html>"""

if __name__ == "__main__":
    main()
