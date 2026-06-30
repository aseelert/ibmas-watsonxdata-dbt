#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  ds_flow_lib.py — build IBM DataStage pipeline-flow (v3) JSON for the medallion demo
#
#  Location  : scripts/datastage/ds_flow_lib.py
#  Project   : watsonx.data · dbt · Spark · Confluent · DataStage medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  WHAT / WHY
#  ----------
#  Programmatically emits CPD 5.3.4 DataStage "pipeline-flow v3" JSON documents
#  (doc_type=pipeline, json_schema=pipeline-flow-v3) that mirror the dbt medallion
#  models. Each dbt model becomes ONE source->target stage pair:
#
#     [ watsonx.data Presto connector | read_mode=select, select_statement=<dbt SQL> ]
#                              | (link)
#                              v
#     [ watsonx.data Presto connector | table_action=replace, write Iceberg table ]
#
#  Both ends use the SAME watsonx.data Presto connection (op="lakehouse"), so the
#  whole flow runs through ONE connection and the transformation is pushed down to
#  Presto as SQL — byte-for-byte parity with the dbt path. There is NO DataStage
#  Python SDK involved: flows are plain JSON created via the Watson Data REST API.
#
#  The node/port/connection JSON shapes here were reverse-engineered from real
#  flows already present in the ibmas-ingest-demo project (the auto-generated
#  "DataStage flow of data rule ..." assets and the hand-built "DS-merge" flow),
#  so they match exactly what this CPD 5.3.4 instance produces in its own canvas.
# -----------------------------------------------------------------------------
from __future__ import annotations
import uuid


# --- Presto type -> DataStage type encoding ---------------------------------
# Maps the Presto/Iceberg column type (from DESCRIBE on the dbt tables) onto the
# twin representations a pipeline-flow needs:
#   * schemas[].fields[]            (rich metadata form, used by the canvas)
#   * node.parameters.*colProperties (compact form, used by the connector to
#                                     build CREATE TABLE DDL on the target)
def _ds_type(presto_type: str):
    t = presto_type.strip().lower()
    # normalise "timestamp with time zone" -> plain timestamp (DataStage has no
    # tz-aware timestamp; the value is a load-time stamp so tz is immaterial).
    if t.startswith("timestamp"):
        return dict(type="timestamp", odbc="TIMESTAMP", code="DATETIME",
                    length=26, precision=6, scale=6, dt="TIMESTAMP", signed=True, unicode=False)
    if t == "integer":
        return dict(type="integer", odbc="INTEGER", code="INT32",
                    length=10, precision=0, scale=0, dt="INTEGER", signed=True, unicode=False)
    if t == "bigint":
        return dict(type="bigint", odbc="BIGINT", code="INT64",
                    length=19, precision=0, scale=0, dt="BIGINT", signed=True, unicode=False)
    if t == "double":
        return dict(type="double", odbc="DOUBLE", code="DFLOAT",
                    length=-1, precision=0, scale=0, dt="DOUBLE", signed=True, unicode=False)
    if t.startswith("decimal"):
        # decimal(p,s)
        p, s = 38, 0
        if "(" in t:
            inside = t[t.index("(") + 1:t.index(")")]
            p, s = (int(x) for x in inside.split(","))
        return dict(type="decimal", odbc="DECIMAL", code="DECIMAL",
                    length=p, precision=p, scale=s, dt="DECIMAL", signed=True, unicode=False)
    if t == "date":
        return dict(type="date", odbc="DATE", code="DATE",
                    length=10, precision=0, scale=0, dt="DATE", signed=True, unicode=False)
    # varchar / everything else -> string
    return dict(type="string", odbc="VARCHAR", code="STRING",
                length=1024, precision=0, scale=0, dt="VARCHAR", signed=False, unicode=False)


def _field(name: str, presto_type: str, nullable: bool = True):
    d = _ds_type(presto_type)
    return {
        "metadata": {
            "is_key": False,
            "min_length": 0,
            "decimal_scale": d["scale"],
            "decimal_precision": d["precision"],
            "is_signed": d["signed"],
            "max_length": d["length"] if d["length"] > 0 else 0,
        },
        "nullable": nullable,
        "name": name,
        "type": d["type"],
        "app_data": {"odbc_type": d["odbc"], "is_unicode_string": d["unicode"], "type_code": d["code"]},
    }


def _colprop(name: str, presto_type: str, nullable: bool = True):
    d = _ds_type(presto_type)
    return {
        "ColumnName": name,
        "DataType": d["dt"],
        "Length": d["length"],
        "Scale": d["scale"],
        "Nullable_check": nullable,
        "Key": False,
    }


def _uid():
    return str(uuid.uuid4())


class FlowBuilder:
    """Accumulates source->target stage pairs and renders one pipeline-flow doc."""

    def __init__(self, connection_ref: str, project_ref: str,
                 connection_name: str = "IBM_watsonx.data_Presto",
                 catalog: str = "iceberg_data"):
        self.conn = connection_ref
        self.proj = project_ref
        self.conn_name = connection_name
        self.catalog = catalog
        self.nodes = []
        self.schemas = []
        self.pipeline_id = _uid()
        self._y = 100  # vertical layout cursor; each pair gets its own row

    def add_model(self, *, label: str, select_statement: str,
                  target_schema: str, target_table: str, columns: list[tuple]):
        """columns = [(name, presto_type), ...] (matches the SELECT output order)."""
        schema_id = _uid()
        self.schemas.append({"id": schema_id, "fields": [_field(n, t) for n, t in columns]})
        colprops = [_colprop(n, t) for n, t in columns]

        src_id, tgt_id = _uid(), _uid()
        src_out_port, tgt_in_port = _uid(), _uid()
        link_id = _uid()
        link_name = f"lnk_{target_table}"
        y = self._y
        self._y += 140

        # ---- source: watsonx.data Presto, custom SQL pushdown ----
        self.nodes.append({
            "id": src_id,
            "type": "binding",
            "op": "lakehouse",
            "app_data": {"ui_data": {
                "label": f"src_{label}",
                "image": "/data-intg/flows/graphics/palette/lakehouse.svg",
                "x_pos": 120, "y_pos": y}},
            "outputs": [{
                "id": src_out_port,
                "schema_ref": schema_id,
                "app_data": {
                    "datastage": {"is_source_of_link": link_id},
                    "ui_data": {"label": "outPort"}},
            }],
            "parameters": {
                "output_count": 1, "input_count": 0, "execmode": "default_par",
                "outputcolProperties": colprops},
            "connection": {
                "ref": self.conn, "project_ref": self.proj,
                "connData": {"op": "existing-asset", "stageName": "lakehouse",
                             "connectionName": self.conn_name},
                "properties": {"read_mode": "select", "select_statement": select_statement}},
        })

        # ---- target: watsonx.data Presto, replace (create) Iceberg table ----
        self.nodes.append({
            "id": tgt_id,
            "type": "binding",
            "op": "lakehouse",
            "app_data": {"ui_data": {
                "label": f"tgt_{label}",
                "image": "/data-intg/flows/graphics/palette/lakehouse.svg",
                "x_pos": 520, "y_pos": y}},
            "inputs": [{
                "id": tgt_in_port,
                "schema_ref": schema_id,
                "app_data": {"ui_data": {"label": "inPort"}},
                "links": [{
                    "id": link_id,
                    "node_id_ref": src_id,
                    "port_id_ref": src_out_port,
                    "link_name": link_name,
                    "type_attr": "PRIMARY",
                    "app_data": {"ui_data": {"decorations": [
                        {"id": "dec-lbl", "position": "middle", "label": link_name,
                         "label_editable": True, "label_single_line": True}]}},
                }],
            }],
            "parameters": {
                "input_count": 1, "output_count": 0, "execmode": "default_par",
                "inputName": link_name,
                "inputcolProperties": colprops},
            "connection": {
                "ref": self.conn, "project_ref": self.proj,
                "connData": {"op": "existing-asset", "stageName": "lakehouse",
                             "connectionName": self.conn_name},
                "properties": {
                    "write_mode": "write", "table_action": "replace",
                    "catalog_name": self.catalog,
                    "schema_name": target_schema, "table_name": target_table}},
        })

    def render(self) -> dict:
        return {
            "doc_type": "pipeline",
            "version": "3.0",
            "json_schema": "http://api.dataplatform.ibm.com/schemas/common-pipeline/pipeline-flow/pipeline-flow-v3-schema.json",
            "primary_pipeline": self.pipeline_id,
            "pipelines": [{
                "id": self.pipeline_id,
                "runtime_ref": "pxOsh",
                "nodes": self.nodes,
                "app_data": {"ui_data": {"comments": []}},
            }],
            "schemas": self.schemas,
            "runtimes": [{"id": "pxOsh", "name": "pxOsh"}],
            "app_data": {},
        }
