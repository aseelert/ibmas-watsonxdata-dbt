# watsonx.data · dbt Medallion — DQ & Lineage Dashboard

A single-file, offline-capable dashboard for **every dbt asset in watsonx.data**,
covering data quality, lineage, record counts, column-level scores, key
relationships and governance (data classes).

> **Every metric here was sourced exclusively from the IBM watsonx.data
> intelligence MCP server** — no direct Presto queries, no dbt-artifact reads.

## Files

| File | What it is |
|------|-----------|
| `dashboard.html` | The dashboard. Self-contained (data inlined) — open it directly, no server or internet needed. |
| `data.json` | The MCP-sourced dataset (also embedded in the HTML). Reusable for other tools. |
| `build_dashboard.py` | Rebuilds `data.json` + `dashboard.html` from the captured MCP responses. |
| `screenshots/` | Reference renders. |

## How to view

Because `dashboard.html` has the data inlined, just open it in a browser:

```bash
open claude-mcp-examples/dashboard.html        # macOS
```

(If your browser blocks the inlined script under `file://`, serve it:
`cd claude-mcp-examples && python3 -m http.server 8899` → http://localhost:8899/dashboard.html)

## What it shows

- **KPIs** — 17 assets · avg DQ 98.23% · 3,236 records profiled · 122 columns · 13 data classes.
- **Medallion pipeline** — the four layers (`lakehouse_demo_ingest` → `_bronze` →
  `_silver` → `_gold`) with each asset's DQ and row count.
- **Lineage & data flow** — a layered graph of the bronze→silver→gold flow,
  including the `silver_sales_enriched` 4-way join and the two gold marts.
- **Asset register** — sortable table: overall DQ + the four DQ dimensions
  (validity / completeness / uniqueness / consistency), records, column count,
  classified-column count, FK suggestions. **Click any row** for column-level
  quality scores, data classes and the DQ dimensions checked per column.
- **Key relationships** — foreign-key relationships inferred from MCP key-analysis
  + matching `Customer Number` / `Identifier` data classes.
- **Governance** — auto-discovered data classes, with PII (🔒 Email Address,
  First/Last Name, Customer Number) flagged.

## MCP tools used

| Tool | Provided |
|------|----------|
| `list_containers` | catalogs + projects inventory |
| `search_asset` | the 17 dbt assets + their IDs in project `ibmas-ingest-demo` |
| `get_data_quality_for_asset` | table-level overall DQ + dimension scores |
| `get_asset_details` | record counts, columns, **per-column quality**, **data classes**, column checks, PK/FK key analysis |
| `get_asset_glossary_artifacts` | confirmed no business *terms* assigned (governance is via data *classes*) |

## Known environment limitations (captured honestly in the dashboard)

- **Lineage graph service unavailable** — `search_lineage_assets` /
  `convert_asset_to_lineage_id` / `get_lineage_graph` all return **HTTP 404** in
  this environment. The medallion lineage shown is therefore **reconstructed from
  MCP metadata** (layer tags + resource-key schema paths + FK key-analysis), not
  from the dedicated lineage API.
- **Data Product Hub not configured** — `search_data_products` returns
  `CATSV5124E: Catalog does not exist for given GUID 'ibm-default-hub'`. No data
  products are wired up yet, so the "data product connections" piece is empty by
  design.
- **Business terms** — none are assigned to assets yet; the glossary signal
  present today is the auto-discovered **data class** layer.
- A few `record_count` values are `null` where MCP profiling did not compute a
  count (`bronze_order_items`, `silver_order_items`); shown as `—`.

To refresh after re-profiling in watsonx, re-run the MCP collection and
`python3 build_dashboard.py`.
