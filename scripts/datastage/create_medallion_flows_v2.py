#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  create_medallion_flows_v2.py — DataStage medallion flows with transformers
#
#  Location  : scripts/datastage/create_medallion_flows_v2.py
#  Project   : watsonx.data · dbt · Spark · Confluent · DataStage medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  FIVE ORDERED FLOWS
#  ------------------
#  ds_medallion_bronze_v2
#    lakehouse(dbt_demo_raw.*) → CTransformerStage(add ingest metadata) → lakehouse(bronze)
#    Pattern: SQL-read raw → transformer stamps DSJobStartTimestamp / DSFlowName / DSJobRunId
#
#  ds_medallion_silver_clean_v2
#    lakehouse(bronze.* filtered) → CTransformerStage(cast/trim/lower/upper/+transformed_at)
#                        → lakehouse(silver.*)
#    4 parallel paths: customers / orders / order_items / products
#
#  ds_medallion_silver_enrich_v2
#    4 lakehouse(silver.*) sources → PxJoin×3 → CTransformerStage(gross/net/+transformed_at)
#      → lakehouse(silver.silver_sales_enriched)
#
#  ds_medallion_gold_daily_v2
#    lakehouse(silver.silver_sales_enriched SQL GROUP BY) → lakehouse(gold.gold_daily_sales)
#
#  ds_medallion_gold_marts_v2
#    lakehouse(silver/gold SQL GROUP BY) → lakehouse(gold.*)
#    2 parallel paths: category_performance (from gold_daily_sales) / customer_360
#
#  Run order: bronze → silver_clean → silver_enrich → gold_daily → gold_marts
#
#  Connections (from live ibmas-ingest-demo project):
#    ibmas-presto  ed04ec2a-84a8-429b-8348-fc8517bb9fad
#    project-id    2d2415ea-71b5-4215-a7b6-b32a4889611e
# -----------------------------------------------------------------------------
from __future__ import annotations
import argparse, json, os, sys, uuid
from pathlib import Path

import requests
from dotenv import load_dotenv

requests.packages.urllib3.disable_warnings()

ROOT      = Path(__file__).resolve().parents[2]
FLOWS_DIR = Path(__file__).resolve().parent / "flows"
load_dotenv(ROOT / ".env")

PROJECT_ID  = "2d2415ea-71b5-4215-a7b6-b32a4889611e"
PRESTO_CONN = "ed04ec2a-84a8-429b-8348-fc8517bb9fad"
CATALOG     = "iceberg_data"
RAW         = "dbt_demo_raw"
BRONZE      = "datastage_demo_bronze_v2"
SILVER      = "datastage_demo_silver_v2"
GOLD        = "datastage_demo_gold_v2"


# ─────────────────────────────────────────────────────────────────────────────
#  Primitive helpers
# ─────────────────────────────────────────────────────────────────────────────
def _uid() -> str:
    return str(uuid.uuid4())


def _decs(label: str) -> list:
    return [
        {"image": "/data-intg/flows/graphics/flows/link-output-handle--default-selected.svg",
         "temporary": False, "outline": False, "distance": 0,
         "x_pos": -10, "id": "dec-3", "position": "source", "y_pos": -10,
         "class_name": "linkStartImage"},
        {"image": "/data-intg/flows/graphics/flows/link-marking--auto-partitioning.svg",
         "temporary": False, "outline": False, "distance": 2, "width": 50,
         "tooltip": "Auto-partition", "x_pos": -15, "id": "dec-6R",
         "position": "middle", "y_pos": 4, "height": 50},
        {"temporary": False, "label_single_line": True, "label_align": "center",
         "tooltip": label, "x_pos": 0, "label": label,
         "label_allow_return_key": "save",
         "width": max(len(label) * 8, 40), "label_editable": True,
         "id": "dec-8", "position": "middle", "y_pos": -20, "height": 20},
    ]


def _field(name: str, ftype: str, odbc: str, code: str,
           length: int = 1024, scale: int = 0, precision: int = 0,
           signed: bool = True, nullable: bool = True,
           deriv: str | None = None) -> dict:
    """Schema field — auto-extracts decimal(p,s) if scale/precision not given."""
    if scale == 0 and "decimal" in ftype.lower() and "," in ftype:
        try:
            inner = ftype[ftype.index("(") + 1: ftype.index(")")]
            p, s  = inner.split(",")
            precision, scale = int(p.strip()), int(s.strip())
        except (ValueError, IndexError):
            pass
    if precision == 0 and length > 0 and "decimal" in ftype.lower():
        precision = length
    m = {"item_index": 0, "is_key": False,
         "min_length": 0, "max_length": max(length, 0),
         "decimal_scale": scale, "decimal_precision": precision,
         "description": " ", "TimeScale": 0, "is_signed": signed}
    if deriv:
        m["customDeriv"] = deriv
    return {"metadata": m, "nullable": nullable, "MisFieldProperties": "",
            "name": name, "isSelected": False, "type": ftype,
            "app_data": {"time_scale": 0, "odbc_type": odbc,
                         "is_unicode_string": False, "type_code": code},
            "LastSchemaColumnName": name}


def _colprop(name: str, dtype: str, length: int = 1024,
             scale: int = 0, signed: bool = True) -> dict:
    """colProperties entry — DECIMAL(p,s) format required by PX runtime."""
    if dtype.upper() == "DECIMAL" and scale > 0:
        meta = f"DECIMAL({length},{scale})"
    elif length > 0:
        meta = f"{dtype}({length})"
    else:
        meta = dtype
    return {"Unicode": False, "Description": " ", "Signed": signed,
            "Metadata": meta, "Scale": scale, "TimeScale": 0,
            "OldColumnName": name, "TempOldColumnName": name,
            "ColumnName": name, "Length": max(length, 0),
            "DataType": dtype, "Key": False, "Nullable": True}


# Column tuple = (name, ftype, odbc, code, length, signed)
# helper to build a _colprop from that tuple, extracting scale from ftype
def _cp(c: tuple) -> dict:
    scale = 0
    if "decimal" in c[1].lower() and "," in c[1]:
        try:
            inner = c[1][c[1].index("(") + 1: c[1].index(")")]
            scale = int(inner.split(",")[1].strip())
        except (ValueError, IndexError):
            pass
    return _colprop(c[0], c[2].upper(), c[4], scale, c[5])


def _wire(src_node: dict, link_id: str) -> None:
    """Set the outgoing link id on a source/transformer node's first output."""
    src_node["outputs"][0]["app_data"]["datastage"]["is_source_of_link"] = link_id


# ─────────────────────────────────────────────────────────────────────────────
#  Node builders
# ─────────────────────────────────────────────────────────────────────────────

def _src_general(label: str, schema: str, table: str,
                 schema_ref: str, colprops: list, x: int, y: int) -> tuple:
    """lakehouse source, read_mode=general (table read). Returns (id, out_port, node)."""
    nid, op = _uid(), _uid()
    return nid, op, {
        "id": nid, "type": "binding", "op": "lakehouse",
        "app_data": {"ui_data": {"image": "/data-intg/flows/graphics/palette/lakehouse.svg",
                                 "x_pos": x, "y_pos": y, "label": label}},
        "outputs": [{"id": op, "schema_ref": schema_ref,
                     "app_data": {"datastage": {}, "ui_data": {"label": "outPort",
                                  "cardinality": {"min": 0, "max": 1}},
                                  "additionalProperties": {"enableAcp": True}},
                     "parameters": {"buf_mode": "default"}}],
        "parameters": {"combinability": "auto", "output_count": 1, "input_count": 0,
                       "execmode": "default_par", "preserve": -3,
                       "outputcolProperties": colprops},
        "connection": {"ref": PRESTO_CONN, "project_ref": PROJECT_ID,
                       "connData": {"op": "existing-asset", "stageName": "lakehouse",
                                    "connectionName": "ibmas-presto"},
                       "properties": {"read_mode": "general", "catalog_name": CATALOG,
                                      "schema_name": schema, "table_name": table,
                                      "outputAcpShouldHide": True,
                                      "enableFlowAcpControl": True,
                                      "node_number": 0, "node_count": 1}}}


def _src_sql(label: str, sql: str,
             schema_ref: str, colprops: list, x: int, y: int) -> tuple:
    """lakehouse source, read_mode=select (SQL pushdown). Returns (id, out_port, node)."""
    nid, op = _uid(), _uid()
    return nid, op, {
        "id": nid, "type": "binding", "op": "lakehouse",
        "app_data": {"ui_data": {"image": "/data-intg/flows/graphics/palette/lakehouse.svg",
                                 "x_pos": x, "y_pos": y, "label": label}},
        "outputs": [{"id": op, "schema_ref": schema_ref,
                     "app_data": {"datastage": {}, "ui_data": {"label": "outPort",
                                  "cardinality": {"min": 0, "max": 1}},
                                  "additionalProperties": {"enableAcp": True}},
                     "parameters": {"buf_mode": "default"}}],
        "parameters": {"combinability": "auto", "output_count": 1, "input_count": 0,
                       "execmode": "default_par", "preserve": -3,
                       "outputcolProperties": colprops},
        "connection": {"ref": PRESTO_CONN, "project_ref": PROJECT_ID,
                       "connData": {"op": "existing-asset", "stageName": "lakehouse",
                                    "connectionName": "ibmas-presto"},
                       "properties": {"read_mode": "select", "select_statement": sql,
                                      "outputAcpShouldHide": True,
                                      "enableFlowAcpControl": True,
                                      "node_number": 0, "node_count": 1}}}


def _xfm(label: str,
         src_id: str, src_op: str, lnk_in: str,
         derived_schema_ref: str, val_derivs: list,
         lnk_out: str, x: int, y: int,
         runtime_column_propagation: int = 1,
         input_schema_ref: str | None = None) -> tuple:
    """CTransformerStage. Returns (id, out_port, node).
    ONLY derived/computed columns go in val_derivs — passthrough columns
    propagate automatically via runtime_column_propagation=1."""
    nid, in_port, out_port, in_link = _uid(), _uid(), _uid(), _uid()
    return nid, out_port, {
        "id": nid, "type": "execution_node", "op": "CTransformerStage",
        "app_data": {"datastage": {"inputs_order": in_port, "outputs_order": out_port},
                     "ui_data": {"image": "/data-intg/flows/graphics/palette/CTransformerStage.svg",
                                 "x_pos": x, "y_pos": y, "label": label}},
        "inputs": [dict({"id": in_port,
                         "app_data": {"ui_data": {"label": "inPort",
                                                  "cardinality": {"min": 1, "max": 1}}},
                         "parameters": {"keyColsPart": [], "perform_sort": False,
                                        "runtime_column_propagation": runtime_column_propagation},
                         "links": [{"id": in_link, "node_id_ref": src_id,
                                    "port_id_ref": src_op, "type_attr": "PRIMARY",
                                    "link_name": lnk_in,
                                    "app_data": {"datastage": {}, "ui_data": {
                                        "decorations": _decs(lnk_in)}}}]},
                        **({"schema_ref": input_schema_ref} if input_schema_ref else {}))],
        "outputs": [{"id": out_port, "schema_ref": derived_schema_ref,
                     "app_data": {"datastage": {}, "ui_data": {"label": "outPort",
                                  "cardinality": {"min": 1, "max": 2147483647}},
                                  "additionalProperties": {"enableAcp": True}},
                     "parameters": {"outputName": lnk_out,
                                    "valueDerivation": val_derivs,
                                    "buf_mode": "default"}}],
        "parameters": {"BlockSizeSelectedType": "systemSelected",
                       "StageVariables": [], "Triggers": [], "LoopVariables": [],
                       "OutputlinkOrderingList": [{"link_label": "0", "link_name": lnk_out}],
                       "InputlinkOrderingList": [{"link_label": "0", "link_name": lnk_in}],
                       "output_count": 1, "input_count": 1,
                       "execmode": "default_par", "combinability": "auto",
                       "SKKeySourceType": "file",
                       "runtime_column_propagation": bool(runtime_column_propagation),
                       "preserve": -3}}


def _pxjoin(label: str,
            left_id: str, left_op: str, lnk_left: str, left_schema_ref: str,
            right_id: str, right_op: str, lnk_right: str, right_schema_ref: str,
            out_schema_ref: str, key_col: str,
            x: int, y: int) -> tuple:
    """PxJoin INNER join on key_col. Returns (id, out_port, node)."""
    nid       = _uid()
    in_left   = _uid(); in_right = _uid()
    lnk_l_id  = _uid(); lnk_r_id = _uid()
    out_port  = _uid(); out_lnk  = _uid()
    return nid, out_port, {
        "id": nid, "type": "execution_node", "op": "PxJoin",
        "app_data": {
            "datastage": {"inputs_order": f"{in_left}|{in_right}",
                          "outputs_order": out_port},
            "ui_data": {"image": "/data-intg/flows/graphics/palette/PxJoin.svg",
                        "x_pos": x, "y_pos": y, "label": label}},
        "inputs": [
            {"id": in_left, "schema_ref": left_schema_ref,
             "app_data": {"ui_data": {"label": "inPort",
                                      "cardinality": {"min": 2, "max": 2147483646}}},
             "parameters": {"runtime_column_propagation": 1},
             "links": [{"id": lnk_l_id, "node_id_ref": left_id,
                        "port_id_ref": left_op, "type_attr": "PRIMARY",
                        "link_name": lnk_left,
                        "app_data": {"datastage": {}, "ui_data": {
                            "decorations": _decs(lnk_left)}}}]},
            {"id": in_right, "schema_ref": right_schema_ref,
             "app_data": {"ui_data": {"label": "inPort",
                                      "cardinality": {"min": 2, "max": 2147483646}}},
             "parameters": {"runtime_column_propagation": 1},
             "links": [{"id": lnk_r_id, "node_id_ref": right_id,
                        "port_id_ref": right_op, "type_attr": "PRIMARY",
                        "link_name": lnk_right,
                        "app_data": {"datastage": {}, "ui_data": {
                            "decorations": _decs(lnk_right)}}}]}],
        "outputs": [{"id": out_port, "schema_ref": out_schema_ref,
                     "app_data": {"datastage": {"is_source_of_link": out_lnk},
                                  "ui_data": {"label": "outPort",
                                              "cardinality": {"min": 1, "max": 1}},
                                  "additionalProperties": {"enableAcp": True}},
                     "parameters": {"buf_mode": "default"}}],
        "parameters": {"operator": "innerjoin",
                       "keyProperties": [{"key": key_col}],
                       "inputName": lnk_left,
                       "InputlinkOrderingList": [{"link_label": "Left",  "link_name": lnk_left},
                                                  {"link_label": "Right", "link_name": lnk_right}],
                       "output_count": 1, "input_count": 2,
                       "execmode": "default_par", "combinability": "auto",
                       "hideCaseSensitiveColumn": True, "showPartType": True,
                       "showCollType": False, "showSortOptions": False,
                       "enableSchemalessDesign": False, "preserve": -3}}


def _tgt(label: str, schema: str, table: str,
         src_id: str, src_op: str, schema_ref: str,
         lnk_name: str, colprops: list, x: int, y: int) -> tuple:
    """lakehouse target (write Iceberg table). Returns (id, node)."""
    nid, in_port, in_link = _uid(), _uid(), _uid()
    return nid, {
        "id": nid, "type": "binding", "op": "lakehouse",
        "app_data": {"ui_data": {"image": "/data-intg/flows/graphics/palette/lakehouse.svg",
                                 "x_pos": x, "y_pos": y, "label": label}},
        "inputs": [{"id": in_port, "schema_ref": schema_ref,
                    "app_data": {"ui_data": {"label": "inPort",
                                             "cardinality": {"min": 0, "max": 1}}},
                    "parameters": {"runtime_column_propagation": 1},
                    "links": [{"id": in_link, "node_id_ref": src_id,
                               "port_id_ref": src_op, "type_attr": "PRIMARY",
                               "link_name": lnk_name,
                               "app_data": {"datastage": {}, "ui_data": {
                                   "decorations": _decs(lnk_name)}}}]}],
        "parameters": {"combinability": "auto", "input_count": 1, "output_count": 0,
                       "execmode": "default_par", "preserve": -3,
                       "inputName": lnk_name, "inputcolProperties": colprops},
        "connection": {"ref": PRESTO_CONN, "project_ref": PROJECT_ID,
                       "connData": {"op": "existing-asset", "stageName": "lakehouse",
                                    "connectionName": "ibmas-presto"},
                       "properties": {"write_mode": "write", "table_action": "append",
                                      "catalog_name": CATALOG,
                                      "schema_name": schema, "table_name": table,
                                      "part_type": "auto", "showPartType": True,
                                      "enableFlowAcpControl": True,
                                      "node_number": 0, "node_count": 1}}}


def _flow(nodes: list, schemas: list) -> dict:
    pid = _uid()
    return {"doc_type": "pipeline", "version": "3.0",
            "json_schema": "http://api.dataplatform.ibm.com/schemas/common-pipeline/pipeline-flow/pipeline-flow-v3-schema.json",
            "primary_pipeline": pid,
            "pipelines": [{"id": pid, "runtime_ref": "pxOsh", "nodes": nodes,
                           "app_data": {"ui_data": {"comments": []}}}],
            "schemas": schemas,
            "runtimes": [{"id": "pxOsh", "name": "pxOsh"}],
            "app_data": {}}


# ─────────────────────────────────────────────────────────────────────────────
#  BRONZE FLOW
#  raw → CTransformerStage (add ingest metadata) → bronze
#  Only the 4 metadata columns are listed in valueDerivation;
#  business columns pass through automatically (runtime_column_propagation=1).
# ─────────────────────────────────────────────────────────────────────────────
def build_bronze() -> dict:
    nodes, schemas = [], []

    # Metadata fields derived by the transformer (mirrors LandingToBronzeFlow)
    meta_fields = [
        _field("_ingested_at",     "timestamp","TIMESTAMP","DATETIME",19, signed=False,nullable=False,deriv="DSJobStartTimestamp"),
        _field("_ingested_by",     "string",   "VARCHAR",  "STRING", 100, signed=False,nullable=False,deriv="DSFlowName"),
        _field("_source_file",     "string",   "VARCHAR",  "STRING", 256, signed=False,nullable=False),
        _field("_ingest_batch_id", "string",   "VARCHAR",  "STRING", 128, signed=False,nullable=False,deriv="DSJobRunId"),
    ]
    meta_cps = [_colprop("_ingested_at","TIMESTAMP",19,signed=False),
                _colprop("_ingested_by","VARCHAR",100,signed=False),
                _colprop("_source_file","VARCHAR",256,signed=False),
                _colprop("_ingest_batch_id","VARCHAR",128,signed=False)]

    tables = [
        ("raw_customers",  "bronze_customers",  "raw_customers.csv",
         [("customer_id","integer","INTEGER","INT32",  10,True),
          ("first_name", "string", "VARCHAR","STRING",255,False),
          ("last_name",  "string", "VARCHAR","STRING",255,False),
          ("email",      "string", "VARCHAR","STRING",255,False),
          ("signup_date","date",   "DATE",   "DATE",   10,False),
          ("country",    "string", "VARCHAR","STRING",  10,False)]),
        ("raw_orders",     "bronze_orders",     "raw_orders.csv",
         [("order_id",      "integer",  "INTEGER",  "INT32",  10,True),
          ("customer_id",   "integer",  "INTEGER",  "INT32",  10,True),
          ("order_ts",      "timestamp","TIMESTAMP","DATETIME",26,False),
          ("status",        "string",   "VARCHAR",  "STRING", 50,False),
          ("payment_method","string",   "VARCHAR",  "STRING", 50,False)]),
        ("raw_order_items","bronze_order_items","raw_order_items.csv",
         [("order_item_id","integer","INTEGER","INT32",10,True),
          ("order_id",     "integer","INTEGER","INT32",10,True),
          ("product_id",   "integer","INTEGER","INT32",10,True),
          ("quantity",     "integer","INTEGER","INT32",10,True),
          ("discount_pct", "double", "DOUBLE", "DFLOAT",0,True)]),
        ("raw_products",   "bronze_products",   "raw_products.csv",
         [("product_id",  "integer","INTEGER","INT32",  10,True),
          ("product_name","string", "VARCHAR","STRING",255,False),
          ("category",    "string", "VARCHAR","STRING",128,False),
          ("unit_price",  "double", "DOUBLE", "DFLOAT",  0,True)]),
    ]

    for i, (raw_t, brz_t, src_f, biz_cols) in enumerate(tables):
        y = 80 + i * 160
        src_sch = _uid(); xfm_sch = _uid(); tgt_sch = _uid()
        src_fields = [_field(c[0],c[1],c[2],c[3],c[4],signed=c[5]) for c in biz_cols]
        src_cps    = [_cp(c) for c in biz_cols]
        schemas += [{"id": src_sch, "fields": src_fields},
                    {"id": xfm_sch, "fields": meta_fields},
                    {"id": tgt_sch, "fields": src_fields + meta_fields}]

        lnk1 = f"Lnk_raw_{raw_t}"; lnk2 = f"Lnk_{brz_t}_out"
        sid, sop, sn = _src_general(f"src_{raw_t}", RAW, raw_t, src_sch, src_cps, 60, y)
        xid, xop, xn = _xfm(f"Xfm_{brz_t}", sid, sop, lnk1, xfm_sch,
                             [{"parsedExpression": "DSJobStartTimestamp", "columnName": "_ingested_at"},
                              {"parsedExpression": "DSFlowName",          "columnName": "_ingested_by"},
                              {"parsedExpression": f"'{src_f}'",           "columnName": "_source_file"},
                              {"parsedExpression": "DSJobRunId",           "columnName": "_ingest_batch_id"}],
                             lnk2, 380, y)
        _wire(sn, xn["inputs"][0]["links"][0]["id"])
        _, tn = _tgt(f"tgt_{brz_t}", BRONZE, brz_t, xid, xop,
                     tgt_sch, lnk2, src_cps + meta_cps, 700, y)
        _wire(xn, tn["inputs"][0]["links"][0]["id"])
        nodes += [sn, xn, tn]

    return _flow(nodes, schemas)


# ─────────────────────────────────────────────────────────────────────────────
#  SILVER CLEAN FLOW  (bronze → transformer → silver, 1:1 per table)
#  Source SQL filters and performs type casts. CTransformerStage performs the
#  visible string cleanup plus transformed_at stamping.
# ─────────────────────────────────────────────────────────────────────────────
def build_silver_clean() -> dict:
    nodes, schemas = [], []

    tables = [
        ("silver_customers",
         (f"SELECT customer_id, first_name, last_name, email, signup_date, country"
          f" FROM {CATALOG}.{BRONZE}.bronze_customers WHERE email IS NOT NULL"),
         [("customer_id","integer","INTEGER","INT32",  10,True),
          ("first_name", "string", "VARCHAR","STRING",255,False),
          ("last_name",  "string", "VARCHAR","STRING",255,False),
          ("email",      "string", "VARCHAR","STRING",255,False),
          ("signup_date","date",   "DATE",   "DATE",   10,False),
          ("country",    "string", "VARCHAR","STRING",  10,False)],
         [("customer_id","integer", "INTEGER","INT32",  10,True),
          ("first_name", "string",  "VARCHAR","STRING",255,False),
          ("last_name",  "string",  "VARCHAR","STRING",255,False),
          ("email",      "string",  "VARCHAR","STRING",255,False),
          ("signup_date","date",    "DATE",   "DATE",   10,False),
          ("country",    "string",  "VARCHAR","STRING",  10,False),
          ("transformed_at","timestamp","TIMESTAMP","DATETIME",26,False)],
         lambda l: [
             ("customer_id",    f"{l}.customer_id"),
             ("first_name",     f"Trim({l}.first_name)"),
             ("last_name",      f"Trim({l}.last_name)"),
             ("email",          f"DownCase(Trim({l}.email))"),
             ("signup_date",    f"{l}.signup_date"),
             ("country",        f"UpCase(Trim({l}.country))"),
             ("transformed_at",  "DSJobStartTimestamp"),
         ]),
        ("silver_orders",
         (f"SELECT cast(order_id as integer) order_id,"
          f" cast(customer_id as integer) customer_id,"
          f" cast(order_ts as timestamp) order_ts,"
          f" cast(cast(order_ts as timestamp) as date) order_date,"
          f" status, payment_method"
          f" FROM {CATALOG}.{BRONZE}.bronze_orders WHERE order_id IS NOT NULL"),
         [("order_id",      "integer",  "INTEGER",  "INT32",  10,True),
          ("customer_id",   "integer",  "INTEGER",  "INT32",  10,True),
          ("order_ts",      "timestamp","TIMESTAMP","DATETIME",26,False),
          ("order_date",    "date",     "DATE",     "DATE",    10,False),
          ("status",        "string",   "VARCHAR",  "STRING",  50,False),
          ("payment_method","string",   "VARCHAR",  "STRING",  50,False)],
         [("order_id",      "integer",  "INTEGER",  "INT32",  10,True),
          ("customer_id",   "integer",  "INTEGER",  "INT32",  10,True),
          ("order_ts",      "timestamp","TIMESTAMP","DATETIME",26,False),
          ("order_date",    "date",     "DATE",     "DATE",    10,False),
          ("status",        "string",   "VARCHAR",  "STRING",  50,False),
          ("payment_method","string",   "VARCHAR",  "STRING",  50,False),
          ("transformed_at","timestamp","TIMESTAMP","DATETIME",26,False)],
         lambda l: [
             ("order_id",       f"{l}.order_id"),
             ("customer_id",    f"{l}.customer_id"),
             ("order_ts",       f"{l}.order_ts"),
             ("order_date",     f"{l}.order_date"),
             ("status",         f"DownCase(Trim({l}.status))"),
             ("payment_method", f"DownCase(Trim({l}.payment_method))"),
             ("transformed_at", "DSJobStartTimestamp"),
         ]),
        ("silver_order_items",
         (f"SELECT cast(order_item_id as integer) order_item_id,"
          f" cast(order_id as integer) order_id,"
          f" cast(product_id as integer) product_id,"
          f" cast(quantity as integer) quantity,"
          f" cast(discount_pct as decimal(5,2)) discount_pct"
          f" FROM {CATALOG}.{BRONZE}.bronze_order_items WHERE quantity > 0"),
         [("order_item_id","integer",     "INTEGER","INT32",  10,True),
          ("order_id",     "integer",     "INTEGER","INT32",  10,True),
          ("product_id",   "integer",     "INTEGER","INT32",  10,True),
          ("quantity",     "integer",     "INTEGER","INT32",  10,True),
          ("discount_pct", "decimal(5,2)","DECIMAL","DECIMAL", 5,True)],
         [("order_item_id","integer",     "INTEGER","INT32",  10,True),
          ("order_id",     "integer",     "INTEGER","INT32",  10,True),
          ("product_id",   "integer",     "INTEGER","INT32",  10,True),
          ("quantity",     "integer",     "INTEGER","INT32",  10,True),
          ("discount_pct", "decimal(5,2)","DECIMAL","DECIMAL", 5,True),
          ("transformed_at","timestamp","TIMESTAMP","DATETIME",26,False)],
         lambda l: [
             ("order_item_id", f"{l}.order_item_id"),
             ("order_id",      f"{l}.order_id"),
             ("product_id",    f"{l}.product_id"),
             ("quantity",      f"{l}.quantity"),
             ("discount_pct",  f"{l}.discount_pct"),
             ("transformed_at", "DSJobStartTimestamp"),
         ]),
        ("silver_products",
         (f"SELECT cast(product_id as integer) product_id,"
          f" product_name, category,"
          f" cast(unit_price as decimal(12,2)) unit_price"
          f" FROM {CATALOG}.{BRONZE}.bronze_products WHERE product_id IS NOT NULL"),
         [("product_id",  "integer",      "INTEGER","INT32",   10,True),
          ("product_name","string",       "VARCHAR","STRING", 255,False),
          ("category",    "string",       "VARCHAR","STRING", 128,False),
          ("unit_price",  "decimal(12,2)","DECIMAL","DECIMAL", 12,True)],
         [("product_id",  "integer",      "INTEGER","INT32",   10,True),
          ("product_name","string",       "VARCHAR","STRING", 255,False),
          ("category",    "string",       "VARCHAR","STRING", 128,False),
          ("unit_price",  "decimal(12,2)","DECIMAL","DECIMAL", 12,True),
          ("transformed_at","timestamp","TIMESTAMP","DATETIME",26,False)],
         lambda l: [
             ("product_id",     f"{l}.product_id"),
             ("product_name",   f"Trim({l}.product_name)"),
             ("category",       f"Trim({l}.category)"),
             ("unit_price",     f"{l}.unit_price"),
             ("transformed_at", "DSJobStartTimestamp"),
         ]),
    ]

    for i, (silv_t, sql, src_cols, out_cols, deriv_fn) in enumerate(tables):
        y = 80 + i * 160
        src_sch = _uid(); xfm_sch = _uid(); tgt_sch = _uid()
        src_fields = [_field(c[0],c[1],c[2],c[3],c[4],signed=c[5]) for c in src_cols]
        out_fields = [_field(c[0],c[1],c[2],c[3],c[4],signed=c[5]) for c in out_cols]
        src_cps    = [_cp(c) for c in src_cols]
        out_cps    = [_cp(c) for c in out_cols]
        schemas += [{"id": src_sch, "fields": src_fields},
                    {"id": xfm_sch, "fields": out_fields},
                    {"id": tgt_sch, "fields": out_fields}]

        lnk1 = f"Lnk_{silv_t}_in"; lnk2 = f"Lnk_{silv_t}_out"
        sid, sop, sn = _src_sql(f"src_{silv_t}", sql, src_sch, src_cps, 60, y)
        xid, xop, xn = _xfm(f"Xfm_{silv_t}", sid, sop, lnk1, xfm_sch,
                             [{"parsedExpression": expr, "columnName": col}
                              for col, expr in deriv_fn(lnk1)],
                             lnk2, 400, y, runtime_column_propagation=0,
                             input_schema_ref=src_sch)
        _wire(sn, xn["inputs"][0]["links"][0]["id"])
        _, tn = _tgt(f"tgt_{silv_t}", SILVER, silv_t, xid, xop,
                     tgt_sch, lnk2, out_cps, 740, y)
        _wire(xn, tn["inputs"][0]["links"][0]["id"])
        nodes += [sn, xn, tn]

    return _flow(nodes, schemas)


# ─────────────────────────────────────────────────────────────────────────────
#  SILVER ENRICH FLOW
#  Reads the four clean silver tables, performs the dbt INNER-join shape, and
#  writes the single analytics-ready silver fact used by every gold mart.
# ─────────────────────────────────────────────────────────────────────────────
def build_silver_enrich() -> dict:
    nodes, schemas = [], []

    oi_cols = [("order_item_id","integer",     "INTEGER","INT32",  10,True),
               ("order_id",     "integer",     "INTEGER","INT32",  10,True),
               ("product_id",   "integer",     "INTEGER","INT32",  10,True),
               ("quantity",     "integer",     "INTEGER","INT32",  10,True),
               ("discount_pct", "decimal(5,2)","DECIMAL","DECIMAL", 5,True),
               ("transformed_at","timestamp",  "TIMESTAMP","DATETIME",26,False)]
    o_cols  = [("order_id",      "integer",  "INTEGER",  "INT32",  10,True),
               ("customer_id",   "integer",  "INTEGER",  "INT32",  10,True),
               ("order_ts",      "timestamp","TIMESTAMP","DATETIME",26,False),
               ("order_date",    "date",     "DATE",     "DATE",   10,False),
               ("status",        "string",   "VARCHAR",  "STRING", 50,False),
               ("payment_method","string",   "VARCHAR",  "STRING", 50,False),
               ("transformed_at","timestamp","TIMESTAMP","DATETIME",26,False)]
    p_cols  = [("product_id",    "integer",      "INTEGER","INT32",   10,True),
               ("product_name",  "string",       "VARCHAR","STRING", 255,False),
               ("category",      "string",       "VARCHAR","STRING", 128,False),
               ("unit_price",    "decimal(12,2)","DECIMAL","DECIMAL", 12,True),
               ("transformed_at","timestamp",    "TIMESTAMP","DATETIME",26,False)]
    c_cols  = [("customer_id",   "integer",  "INTEGER","INT32",  10,True),
               ("first_name",    "string",   "VARCHAR","STRING",255,False),
               ("last_name",     "string",   "VARCHAR","STRING",255,False),
               ("email",         "string",   "VARCHAR","STRING",255,False),
               ("signup_date",   "date",     "DATE",   "DATE",   10,False),
               ("country",       "string",   "VARCHAR","STRING",  10,False),
               ("transformed_at","timestamp","TIMESTAMP","DATETIME",26,False)]

    j1_cols = oi_cols + [c for c in o_cols if c[0] not in {"order_id", "transformed_at"}]
    j2_cols = j1_cols + [c for c in p_cols if c[0] not in {"product_id", "transformed_at"}]
    j3_cols = j2_cols + [c for c in c_cols if c[0] not in {"customer_id", "transformed_at"}]

    enr_cols = [
        ("order_item_id",  "integer",      "INTEGER",  "INT32",    10,True),
        ("order_id",       "integer",      "INTEGER",  "INT32",    10,True),
        ("order_date",     "date",         "DATE",     "DATE",     10,False),
        ("order_ts",       "timestamp",    "TIMESTAMP","DATETIME", 26,False),
        ("status",         "string",       "VARCHAR",  "STRING",   50,False),
        ("payment_method", "string",       "VARCHAR",  "STRING",   50,False),
        ("customer_id",    "integer",      "INTEGER",  "INT32",    10,True),
        ("customer_country","string",       "VARCHAR",  "STRING",   10,False),
        ("product_id",     "integer",      "INTEGER",  "INT32",    10,True),
        ("product_name",   "string",       "VARCHAR",  "STRING",  255,False),
        ("category",       "string",       "VARCHAR",  "STRING",  128,False),
        ("quantity",       "integer",      "INTEGER",  "INT32",    10,True),
        ("unit_price",     "decimal(12,2)","DECIMAL",  "DECIMAL",  12,True),
        ("discount_pct",   "decimal(5,2)", "DECIMAL",  "DECIMAL",   5,True),
        ("gross_amount",   "decimal(14,2)","DECIMAL",  "DECIMAL",  14,True),
        ("net_amount",     "decimal(14,2)","DECIMAL",  "DECIMAL",  14,True),
        ("transformed_at", "timestamp",    "TIMESTAMP","DATETIME", 26,False),
    ]

    def mk_sch(cols):
        sid = _uid()
        schemas.append({"id": sid, "fields": [_field(c[0],c[1],c[2],c[3],c[4],signed=c[5])
                                               for c in cols]})
        return sid

    oi_sch = mk_sch(oi_cols); o_sch = mk_sch(o_cols)
    p_sch = mk_sch(p_cols); c_sch = mk_sch(c_cols)
    j1_sch = mk_sch(j1_cols); j2_sch = mk_sch(j2_cols); j3_sch = mk_sch(j3_cols)
    xfm_sch = mk_sch(enr_cols); enr_sch = mk_sch(enr_cols)
    enr_cps = [_cp(c) for c in enr_cols]

    oi_id, oi_op, oi_n = _src_general("src_silver_order_items", SILVER, "silver_order_items",
                                       oi_sch, [_cp(c) for c in oi_cols], 60, 100)
    o_id, o_op, o_n = _src_general("src_silver_orders", SILVER, "silver_orders",
                                   o_sch, [_cp(c) for c in o_cols], 60, 280)
    p_id, p_op, p_n = _src_general("src_silver_products", SILVER, "silver_products",
                                   p_sch, [_cp(c) for c in p_cols], 60, 460)
    c_id, c_op, c_n = _src_general("src_silver_customers", SILVER, "silver_customers",
                                   c_sch, [_cp(c) for c in c_cols], 60, 640)
    nodes += [oi_n, o_n, p_n, c_n]

    j1_id, j1_op, j1_n = _pxjoin("Join_oi_o",
                                  oi_id, oi_op, "Lnk_oi", oi_sch,
                                  o_id, o_op, "Lnk_o", o_sch,
                                  j1_sch, "order_id", 400, 180)
    _wire(oi_n, j1_n["inputs"][0]["links"][0]["id"])
    _wire(o_n, j1_n["inputs"][1]["links"][0]["id"])
    nodes.append(j1_n)

    j2_id, j2_op, j2_n = _pxjoin("Join_j1_p",
                                  j1_id, j1_op, "Lnk_j1", j1_sch,
                                  p_id, p_op, "Lnk_p", p_sch,
                                  j2_sch, "product_id", 400, 380)
    _wire(j1_n, j2_n["inputs"][0]["links"][0]["id"])
    _wire(p_n, j2_n["inputs"][1]["links"][0]["id"])
    nodes.append(j2_n)

    j3_id, j3_op, j3_n = _pxjoin("Join_j2_c",
                                  j2_id, j2_op, "Lnk_j2", j2_sch,
                                  c_id, c_op, "Lnk_c", c_sch,
                                  j3_sch, "customer_id", 400, 560)
    _wire(j2_n, j3_n["inputs"][0]["links"][0]["id"])
    _wire(c_n, j3_n["inputs"][1]["links"][0]["id"])
    nodes.append(j3_n)

    lnk_in = "Lnk_j3_xfm"; lnk_out = "Lnk_enrich_out"
    derivs = [
        ("order_item_id",      f"{lnk_in}.order_item_id"),
        ("order_id",           f"{lnk_in}.order_id"),
        ("order_date",         f"{lnk_in}.order_date"),
        ("order_ts",           f"{lnk_in}.order_ts"),
        ("status",             f"{lnk_in}.status"),
        ("payment_method",     f"{lnk_in}.payment_method"),
        ("customer_id",        f"{lnk_in}.customer_id"),
        ("customer_country",   f"{lnk_in}.country"),
        ("product_id",         f"{lnk_in}.product_id"),
        ("product_name",       f"{lnk_in}.product_name"),
        ("category",           f"{lnk_in}.category"),
        ("quantity",           f"{lnk_in}.quantity"),
        ("unit_price",         f"{lnk_in}.unit_price"),
        ("discount_pct",       f"{lnk_in}.discount_pct"),
        ("gross_amount",       f"{lnk_in}.quantity * {lnk_in}.unit_price"),
        ("net_amount",         f"{lnk_in}.quantity * {lnk_in}.unit_price * (1 - {lnk_in}.discount_pct)"),
        ("transformed_at",     "DSJobStartTimestamp"),
    ]
    xfm_id, xfm_op, xfm_n = _xfm(
        "Xfm_enrich_sales",
        j3_id, j3_op, lnk_in, xfm_sch,
        [{"parsedExpression": expr, "columnName": col} for col, expr in derivs],
        lnk_out, 640, 380, runtime_column_propagation=0,
        input_schema_ref=j3_sch)
    _wire(j3_n, xfm_n["inputs"][0]["links"][0]["id"])
    nodes.append(xfm_n)

    _, tgt_n = _tgt("tgt_silver_sales_enriched", SILVER, "silver_sales_enriched",
                    xfm_id, xfm_op, enr_sch, lnk_out, enr_cps, 900, 380)
    _wire(xfm_n, tgt_n["inputs"][0]["links"][0]["id"])
    nodes.append(tgt_n)

    return _flow(nodes, schemas)


# ─────────────────────────────────────────────────────────────────────────────
#  GOLD FLOW — SQL pushdown GROUP BY aggregations
# ─────────────────────────────────────────────────────────────────────────────
def build_gold(model_names: set[str] | None = None) -> dict:
    nodes, schemas = [], []

    gold_models = [
        ("gold_daily_sales",
         [("order_date", "date",         "DATE",    "DATE",    10,False),
          ("category",   "string",       "VARCHAR", "STRING", 128,False),
          ("order_count","bigint",       "BIGINT",  "INT64",   19,True),
          ("units_sold", "bigint",       "BIGINT",  "INT64",   19,True),
          ("net_revenue","decimal(14,2)","DECIMAL", "DECIMAL", 14,True)],
         (f"SELECT order_date, category,"
          f" count(distinct order_id) order_count,"
          f" sum(quantity) units_sold,"
          f" cast(sum(net_amount) as decimal(14,2)) net_revenue"
          f" FROM {CATALOG}.{SILVER}.silver_sales_enriched"
          f" WHERE status = 'completed' GROUP BY 1, 2")),
        ("gold_category_performance",
         [("category",           "string",       "VARCHAR","STRING",128,False),
          ("total_orders",       "bigint",       "BIGINT", "INT64",  19,True),
          ("total_units",        "bigint",       "BIGINT", "INT64",  19,True),
          ("total_revenue",      "decimal(14,2)","DECIMAL","DECIMAL",14,True),
          ("avg_revenue_per_unit","decimal(14,2)","DECIMAL","DECIMAL",14,True)],
         (f"SELECT category,"
          f" sum(order_count) total_orders,"
          f" sum(units_sold) total_units,"
          f" cast(sum(net_revenue) as decimal(14,2)) total_revenue,"
          f" cast(sum(net_revenue)/nullif(sum(units_sold),0) as decimal(14,2)) avg_revenue_per_unit"
          f" FROM {CATALOG}.{GOLD}.gold_daily_sales GROUP BY category")),
        ("gold_customer_360",
         [("customer_id",            "integer",     "INTEGER",  "INT32",   10,True),
          ("first_name",             "string",      "VARCHAR",  "STRING", 255,False),
          ("last_name",              "string",      "VARCHAR",  "STRING", 255,False),
          ("email",                  "string",      "VARCHAR",  "STRING", 255,False),
          ("country",                "string",      "VARCHAR",  "STRING",  10,False),
          ("signup_date",            "date",        "DATE",     "DATE",    10,False),
          ("completed_orders",       "bigint",      "BIGINT",   "INT64",   19,True),
          ("returned_orders",        "bigint",      "BIGINT",   "INT64",   19,True),
          ("pending_orders",         "bigint",      "BIGINT",   "INT64",   19,True),
          ("cancelled_orders",       "bigint",      "BIGINT",   "INT64",   19,True),
          ("lifetime_value",         "decimal(14,2)","DECIMAL", "DECIMAL", 14,True),
          ("last_completed_order_ts","timestamp",   "TIMESTAMP","DATETIME",26,False),
          ("last_activity_ts",       "timestamp",   "TIMESTAMP","DATETIME",26,False)],
         (f"WITH m AS ("
          f" SELECT customer_id,"
          f"  count(distinct case when status='completed' then order_id end) completed_orders,"
          f"  count(distinct case when status='returned'  then order_id end) returned_orders,"
          f"  count(distinct case when status='pending'   then order_id end) pending_orders,"
          f"  count(distinct case when status='cancelled' then order_id end) cancelled_orders,"
          f"  cast(coalesce(sum(case when status='completed' then net_amount else 0 end),0)"
          f"    as decimal(14,2)) lifetime_value,"
          f"  max(case when status='completed' then order_ts end) last_completed_order_ts,"
          f"  max(order_ts) last_activity_ts"
          f"  FROM {CATALOG}.{SILVER}.silver_sales_enriched GROUP BY customer_id)"
          f" SELECT c.customer_id, c.first_name, c.last_name, c.email, c.country, c.signup_date,"
          f"  coalesce(m.completed_orders,0) completed_orders,"
          f"  coalesce(m.returned_orders, 0) returned_orders,"
          f"  coalesce(m.pending_orders,  0) pending_orders,"
          f"  coalesce(m.cancelled_orders,0) cancelled_orders,"
          f"  coalesce(m.lifetime_value,  0) lifetime_value,"
          f"  m.last_completed_order_ts, m.last_activity_ts"
          f" FROM {CATALOG}.{SILVER}.silver_customers c"
          f" LEFT JOIN m ON c.customer_id = m.customer_id")),
    ]

    selected_models = [m for m in gold_models if model_names is None or m[0] in model_names]
    for i, (name, cols, sql) in enumerate(selected_models):
        y   = 80 + i * 180
        sch = _uid()
        schemas.append({"id": sch, "fields": [_field(c[0],c[1],c[2],c[3],c[4],signed=c[5])
                                               for c in cols]})
        cps = [_cp(c) for c in cols]
        sid, sop, sn = _src_sql(f"src_{name}", sql, sch, cps, 60, y)
        _, tn = _tgt(f"tgt_{name}", GOLD, name, sid, sop, sch, f"Lnk_{name}", cps, 680, y)
        _wire(sn, tn["inputs"][0]["links"][0]["id"])
        nodes += [sn, tn]

    return _flow(nodes, schemas)


def build_gold_daily() -> dict:
    return build_gold({"gold_daily_sales"})


def build_gold_marts() -> dict:
    return build_gold({"gold_category_performance", "gold_customer_360"})


# ─────────────────────────────────────────────────────────────────────────────
#  CPD helpers
# ─────────────────────────────────────────────────────────────────────────────
def _ca():
    return os.getenv("WXD_SSL_VERIFY", "certs/watsonxdata-ca.pem")


def _token():
    host = os.environ["WXD_CPD_HOST"]
    r = requests.post(f"https://{host}/icp4d-api/v1/authorize",
                      json={"username": os.getenv("WXD_CPD_USERNAME","cpadmin"),
                            "api_key":  os.environ["WXD_API_KEY"]}, verify=_ca())
    r.raise_for_status(); return r.json()["token"]


def _presto():
    import prestodb
    u = os.environ["WXD_USER"]
    c = prestodb.dbapi.connect(
        host=os.environ["WXD_HOST"], port=int(os.getenv("WXD_PORT","443")),
        user=u, catalog=CATALOG, http_scheme="https",
        auth=prestodb.auth.BasicAuthentication(u, os.environ["WXD_API_KEY"]),
        http_headers={"LhInstanceId": os.environ["WXD_INSTANCE_ID"]})
    c._http_session.verify = _ca(); return c


# Map each flow to the target tables it writes
FLOW_TABLES = {
    "ds_medallion_bronze_v2": [
        (BRONZE, "bronze_customers"),
        (BRONZE, "bronze_orders"),
        (BRONZE, "bronze_order_items"),
        (BRONZE, "bronze_products"),
    ],
    "ds_medallion_silver_clean_v2": [
        (SILVER, "silver_customers"),
        (SILVER, "silver_orders"),
        (SILVER, "silver_order_items"),
        (SILVER, "silver_products"),
    ],
    "ds_medallion_silver_enrich_v2": [
        (SILVER, "silver_sales_enriched"),
    ],
    "ds_medallion_gold_daily_v2": [
        (GOLD, "gold_daily_sales"),
    ],
    "ds_medallion_gold_marts_v2": [
        (GOLD, "gold_category_performance"),
        (GOLD, "gold_customer_360"),
    ],
}


def _ensure_schemas(cur):
    for s in (BRONZE, SILVER, GOLD):
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
        print(f"  schema ready: {CATALOG}.{s}")


# DDL for every target table — used by _reset_flow_tables to drop/recreate via
# Presto so DataStage appends into empty Iceberg tables and skips connector-side
# Iceberg REST create/replace calls.
_TABLE_DDL = {
    # ── BRONZE ────────────────────────────────────────────────────────────────
    f"{CATALOG}.{BRONZE}.bronze_customers": (
        "customer_id INTEGER, first_name VARCHAR, last_name VARCHAR, email VARCHAR,"
        " signup_date DATE, country VARCHAR,"
        " _ingested_at TIMESTAMP, _ingested_by VARCHAR, _source_file VARCHAR, _ingest_batch_id VARCHAR"),
    f"{CATALOG}.{BRONZE}.bronze_orders": (
        "order_id INTEGER, customer_id INTEGER, order_ts TIMESTAMP, status VARCHAR,"
        " payment_method VARCHAR,"
        " _ingested_at TIMESTAMP, _ingested_by VARCHAR, _source_file VARCHAR, _ingest_batch_id VARCHAR"),
    f"{CATALOG}.{BRONZE}.bronze_order_items": (
        "order_item_id INTEGER, order_id INTEGER, product_id INTEGER, quantity INTEGER,"
        " discount_pct DOUBLE,"
        " _ingested_at TIMESTAMP, _ingested_by VARCHAR, _source_file VARCHAR, _ingest_batch_id VARCHAR"),
    f"{CATALOG}.{BRONZE}.bronze_products": (
        "product_id INTEGER, product_name VARCHAR, category VARCHAR, unit_price DOUBLE,"
        " _ingested_at TIMESTAMP, _ingested_by VARCHAR, _source_file VARCHAR, _ingest_batch_id VARCHAR"),
    # ── SILVER CLEAN ─────────────────────────────────────────────────────────
    f"{CATALOG}.{SILVER}.silver_customers": (
        "customer_id INTEGER, first_name VARCHAR, last_name VARCHAR, email VARCHAR,"
        " signup_date DATE, country VARCHAR, transformed_at TIMESTAMP"),
    f"{CATALOG}.{SILVER}.silver_orders": (
        "order_id INTEGER, customer_id INTEGER, order_ts TIMESTAMP, order_date DATE,"
        " status VARCHAR, payment_method VARCHAR, transformed_at TIMESTAMP"),
    f"{CATALOG}.{SILVER}.silver_order_items": (
        "order_item_id INTEGER, order_id INTEGER, product_id INTEGER, quantity INTEGER,"
        " discount_pct DECIMAL(5,2), transformed_at TIMESTAMP"),
    f"{CATALOG}.{SILVER}.silver_products": (
        "product_id INTEGER, product_name VARCHAR, category VARCHAR,"
        " unit_price DECIMAL(12,2), transformed_at TIMESTAMP"),
    # ── SILVER ENRICH ─────────────────────────────────────────────────────────
    f"{CATALOG}.{SILVER}.silver_sales_enriched": (
        "order_item_id INTEGER, order_id INTEGER, order_date DATE, order_ts TIMESTAMP,"
        " status VARCHAR, payment_method VARCHAR, customer_id INTEGER, customer_country VARCHAR,"
        " product_id INTEGER, product_name VARCHAR, category VARCHAR,"
        " quantity INTEGER, unit_price DECIMAL(12,2), discount_pct DECIMAL(5,2),"
        " gross_amount DECIMAL(14,2), net_amount DECIMAL(14,2), transformed_at TIMESTAMP"),
    # ── GOLD ──────────────────────────────────────────────────────────────────
    f"{CATALOG}.{GOLD}.gold_daily_sales": (
        "order_date DATE, category VARCHAR, order_count BIGINT, units_sold BIGINT,"
        " net_revenue DECIMAL(14,2)"),
    f"{CATALOG}.{GOLD}.gold_category_performance": (
        "category VARCHAR, total_orders BIGINT, total_units BIGINT,"
        " total_revenue DECIMAL(14,2), avg_revenue_per_unit DECIMAL(14,2)"),
    f"{CATALOG}.{GOLD}.gold_customer_360": (
        "customer_id INTEGER, first_name VARCHAR, last_name VARCHAR, email VARCHAR,"
        " country VARCHAR, signup_date DATE,"
        " completed_orders BIGINT, returned_orders BIGINT, pending_orders BIGINT,"
        " cancelled_orders BIGINT, lifetime_value DECIMAL(14,2),"
        " last_completed_order_ts TIMESTAMP, last_activity_ts TIMESTAMP"),
}


def _reset_flow_tables(cur, flow_name: str):
    """Drop + recreate target tables via Presto so DataStage append finds them.
    The DataStage lakehouse connector only calls Iceberg REST CREATE when the
    table doesn't exist; pre-creating via Presto skips that external-route call."""
    for schema, table in FLOW_TABLES.get(flow_name, []):
        fqn = f"{CATALOG}.{schema}.{table}"
        try:
            cur.execute(f"DROP TABLE IF EXISTS {fqn}")
            print(f"  dropped:   {fqn}")
        except Exception as e:
            print(f"  drop warn ({table}): {e}")
        ddl = _TABLE_DDL.get(fqn)
        if ddl:
            try:
                cur.execute(f"CREATE TABLE {fqn} ({ddl})"
                            f" WITH (format = 'PARQUET')")
                print(f"  created:   {fqn}")
            except Exception as e:
                print(f"  create warn ({table}): {e}")
        else:
            print(f"  no DDL for {fqn} — skipping create")


def _post_flow(host, token, name, doc):
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    v = _ca()
    s = requests.post(
        f"https://{host}/v2/asset_types/data_intg_flow/search?project_id={PROJECT_ID}",
        headers=h, json={"query": f'asset.name:"{name}"', "limit": 5}, verify=v).json()
    for r in s.get("results", []):
        requests.delete(f"https://{host}/v2/assets/{r['metadata']['asset_id']}"
                        f"?project_id={PROJECT_ID}", headers=h, verify=v)
        print(f"  deleted old {name}")
    r = requests.post(
        f"https://{host}/data_intg/v3/data_intg_flows"
        f"?project_id={PROJECT_ID}&data_intg_flow_name={name}",
        headers=h, data=json.dumps({"pipeline_flows": doc}), verify=v)
    if r.status_code in (200, 201):
        aid = r.json()["metadata"]["asset_id"]
        print(f"  CREATED {name}  →  {aid}"); return aid
    print(f"  FAILED  {name}  →  {r.status_code}  {r.text[:300]}"); return ""


def _compile(host, token, fid, name):
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    print(f"  compiling {name} ...", end=" ", flush=True)
    r = requests.post(
        f"https://{host}/data_intg/v3/ds_codegen/compile/{fid}"
        f"?project_id={PROJECT_ID}", headers=h, verify=_ca(), timeout=180)
    if r.status_code in (200, 201):
        print("OK"); return True
    print(f"FAILED  {r.status_code}")
    for ex in r.json().get("message", {}).get("exceptions", []):
        print(f"    [{ex.get('error_node')}] {ex.get('error_message','')[:160]}")
    return False


def _run_job(host, token, fid, name):
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # delete existing job
    sj = requests.post(
        f"https://{host}/v2/asset_types/job/search?project_id={PROJECT_ID}",
        headers=h, json={"query": f'asset.name:"{name}_job"', "limit": 5},
        verify=_ca()).json()
    for r in sj.get("results", []):
        requests.delete(f"https://{host}/v2/assets/{r['metadata']['asset_id']}"
                        f"?project_id={PROJECT_ID}", headers=h, verify=_ca())
    r = requests.post(
        f"https://{host}/v2/jobs?project_id={PROJECT_ID}",
        headers=h,
        json={"job": {"asset_ref": fid, "name": f"{name}_job",
                      "configuration": {"env_type": "dsxlocal-px"}}},
        verify=_ca(), timeout=60)
    if r.status_code not in (200, 201):
        print(f"  job create failed: {r.status_code}"); return "", ""
    jid = r.json()["metadata"]["asset_id"]
    print(f"  job: {name}_job ({jid})")
    rr = requests.post(f"https://{host}/v2/jobs/{jid}/runs?project_id={PROJECT_ID}",
                       headers=h, json={"job_run": {}}, verify=_ca(), timeout=60)
    if rr.status_code not in (200, 201):
        print(f"  run failed: {rr.status_code}"); return jid, ""
    rid = rr.json()["metadata"]["asset_id"]
    print(f"  run: {rid}"); return jid, rid


def _poll(host, token, jid, rid, name, timeout=600):
    import time
    h = {"Authorization": f"Bearer {token}"}
    print(f"  polling {name} ...", end="", flush=True)
    for _ in range(timeout // 10):
        time.sleep(10)
        r = requests.get(
            f"https://{host}/v2/jobs/{jid}/runs/{rid}?project_id={PROJECT_ID}",
            headers=h, verify=_ca())
        state = r.json().get("entity", {}).get("job_run", {}).get("state", "")
        print(f" {state}", end="", flush=True)
        if state in ("Completed", "Failed", "Canceled", "Error"):
            print(); return state == "Completed"
    print(" TIMEOUT"); return False


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
FLOW_DEFS = [
    ("ds_medallion_bronze_v2",        build_bronze),
    ("ds_medallion_silver_clean_v2",  build_silver_clean),
    ("ds_medallion_silver_enrich_v2", build_silver_enrich),
    ("ds_medallion_gold_daily_v2",    build_gold_daily),
    ("ds_medallion_gold_marts_v2",    build_gold_marts),
]


def main():
    ap = argparse.ArgumentParser(description="Build + run DataStage medallion flows (v2).")
    ap.add_argument("--build",  action="store_true")
    ap.add_argument("--create", action="store_true")
    ap.add_argument("--run",    action="store_true", help="with --create: compile + run in order")
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()
    if not (args.build or args.create or args.verify):
        ap.error("pass at least one of --build / --create [--run] / --verify")

    docs = {}
    if args.build or args.create:
        print("Building pipeline-flow JSON:")
        for name, fn in FLOW_DEFS:
            doc = fn()
            docs[name] = doc
            FLOWS_DIR.mkdir(parents=True, exist_ok=True)
            p = FLOWS_DIR / f"{name}.json"
            p.write_text(json.dumps(doc, indent=2))
            n = len(doc["pipelines"][0]["nodes"])
            print(f"  {p.relative_to(ROOT)}  ({n} nodes)")

    if args.create:
        print("\nEnsuring Presto schemas:")
        _ensure_schemas(_presto().cursor())
        print("\nPosting flows to CPD:")
        token = _token(); host = os.environ["WXD_CPD_HOST"]
        fids = {}
        for name, _ in FLOW_DEFS:
            if name in docs:
                aid = _post_flow(host, token, name, docs[name])
                if aid: fids[name] = aid

        if args.run and fids:
            print("\nCompiling and running flows in order:")
            presto_cur = _presto().cursor()
            for name, _ in FLOW_DEFS:
                fid = fids.get(name)
                if not fid:
                    print(f"  SKIP {name}"); continue
                print(f"\n── {name} ──────────────────────────────")
                print(f"  resetting target tables for {name} ...")
                _reset_flow_tables(presto_cur, name)
                if not _compile(host, token, fid, name):
                    print("  compile failed — stopping"); sys.exit(1)
                jid, rid = _run_job(host, token, fid, name)
                if not rid:
                    print("  could not start run — stopping"); sys.exit(1)
                if not _poll(host, token, jid, rid, name):
                    print(f"  {name} failed — stopping"); sys.exit(1)
                print(f"  {name} ✓")
        elif not args.run:
            print("\nFlows created. Run with --run to compile + execute in order:")
            print("  python scripts/datastage/create_medallion_flows_v2.py --create --run")

    if args.verify:
        print("\nParity check (row counts vs dbt):")
        cur = _presto().cursor()
        ok_all = True
        models = [("bronze",BRONZE), ("bronze",BRONZE), ("bronze",BRONZE), ("bronze",BRONZE),
                  ("silver",SILVER), ("silver",SILVER), ("silver",SILVER), ("silver",SILVER),
                  ("silver",SILVER), ("gold",GOLD), ("gold",GOLD), ("gold",GOLD)]
        names  = ["bronze_customers","bronze_orders","bronze_order_items","bronze_products",
                  "silver_customers","silver_orders","silver_order_items","silver_products",
                  "silver_sales_enriched",
                  "gold_daily_sales","gold_category_performance","gold_customer_360"]
        print(f"  {'model':<32} {'ds_rows':>8} {'dbt_rows':>9}  match")
        print("  " + "-" * 57)
        for (layer, ds_schema), m in zip(models, names):
            dbt_schema = f"dbt_demo_{layer}"
            try:
                cur.execute(f"SELECT count(*) FROM {CATALOG}.{ds_schema}.{m}")
                ds = cur.fetchone()[0]
                cur.execute(f"SELECT count(*) FROM {CATALOG}.{dbt_schema}.{m}")
                dbt = cur.fetchone()[0]
                ok = ds == dbt; ok_all &= ok
                print(f"  {m:<32} {ds:>8} {dbt:>9}  {'OK' if ok else 'MISMATCH'}")
            except Exception as e:
                print(f"  {m:<32}  ERROR: {e}"); ok_all = False
        print(f"\n  PARITY: {'ALL MATCH ✓' if ok_all else 'DIFFERENCES FOUND ✗'}")


if __name__ == "__main__":
    main()
