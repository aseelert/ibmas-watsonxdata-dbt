# IKC Governance — Retail Medallion Lakehouse (CPD / IBM Software Hub 5.3.4)

Business glossary, classifications, data classes, reference data and rules for the
watsonx.data medallion demo, authored for **IBM Knowledge Catalog (watsonx.data Intelligence)**
and applied to the **`ibmas-catalog`** catalog (`d742df95-bfe8-4402-a4f3-ce2d18b1c7fb`).

The four datasets (customers, products, orders, order_items) flow Bronze → Silver → Gold in the
dbt / Spark / Confluent paths, so each business term describes a concept identical in every layer.

## Category structure

```
Retail Medallion Lakehouse                    (top-level)
├── Customer      Customer ID · Customer Email · Customer Country · Customer 360
├── Product       Product ID · Product Category · Unit Price
├── Order         Order ID · Order Status · Order Quantity · Net Amount
├── Gold Sales    Daily Sales · Net Revenue · Category Performance
└── Data Governance
    ├── Business Data   (classification)
    └── Personal Data   (classification, child of Business Data)
```

## Files

| Step | File | Artifact type | Notes |
|------|------|---------------|-------|
| 1 | `01_categories.csv` | category | 6 categories incl. `Data Governance` sub-cat |
| 2 | `03_classifications.csv` | classification | `Business Data` + `Personal Data` |
| 3 | `02_business_terms.csv` | glossary_term | 14 terms — **no** data-class or related-term links (base pass) |
| 4 | `04_data_classes.csv` | data_class | 14 custom regex DCs, each linked to the matching business term |

> Files 05 and 06 (reference data sets, rules) exist in this folder but are not part of the
> core import — apply them separately after the 4-step cycle is complete.

## Import order (upload one file at a time, publish after each)

The order breaks every circular dependency:

```
Step 1  Import 01_categories.csv        UI: Governance → Categories → Import
        Publish workflow tasks

Step 2  Import 03_classifications.csv   UI: Governance → Classifications → Import
        Publish workflow tasks
        (Business Data and Personal Data are now live — can be referenced)

Step 3  Import 02_business_terms.csv    UI: Governance → Business terms → Import  (merge: all)
        Publish workflow tasks
        (14 terms are now live — can be referenced by data classes)

Step 4  Import 04_data_classes.csv      UI: Governance → Data classes → Import  (merge: all)
        Publish workflow tasks
        (DCs are live and linked to published terms via Related Terms)
```

**Why this order?**
- Classifications must exist before business terms reference them.
- Business terms must be **published** before data classes can resolve their `Related Terms` column.
- Data classes have no back-reference from the term side — the `ColumnNameFilter` regex handles
  automatic column-to-DC matching during metadata enrichment/profiling.

## Confirmed import rules for CPD 5.3.4 (hard-won, not obvious)

1. **Category path separator is `>>`** — e.g. `Retail Medallion Lakehouse >> Customer`.
   NOT `/` (the MCP schema says `/` — it is **wrong** for this backend) and NOT single `>`.
   Names may not contain `>`.
2. A term's `Category` must be the **full path** from the top-level category.
3. **Multiple values = multiple rows.** Tags, classifications, related terms all use continuation
   rows with only the new value filled in, everything else blank. A comma-separated list in one
   cell is read as ONE name and fails (`GIM00015E`).
4. Import categories before terms. Terms resolve against categories that already exist.
5. **`merge_option=all`** overwrites in place — safe to re-import.
6. **MCP `import_glossary_from_csv` supports only `category` and `glossary_term`.**
   Classifications and data classes must use the native UI import or the REST API endpoint
   `POST /v3/governance_artifact_types/{type}/import?merge_option=all`.
7. New artifacts land in **draft** (`IMPORT_CREATE`) and must be **published** to become active.
8. `GIM00015E: Artifact X not found in hierarchy` = the referenced artifact is either in draft
   or does not exist yet. Fix: publish the dependency first, then re-import.

## Data class design

Each of the 14 data classes maps 1-to-1 with a business term and contains:

- **`RegularExpressionClassifier`** — value-level regex derived from the seed CSV sample data
- **`ColumnNameFilter`** — exact column name regex (e.g. `^customer_id$`) so WKC profiling
  auto-detects the right column during metadata enrichment
- **`Classifications`** — `Business Data` for all; additionally `Personal Data` for customer_id
  and email
- **`Related Terms`** — points to the matching published business term

| Data class | Column matched | Value regex |
|------------|---------------|-------------|
| Customer ID | `customer_id` | `^[0-9]{4}$` |
| Customer Email | `email` | RFC 5321 email pattern |
| Customer Country | `country` | `^(US\|GB\|DE\|FR\|IT\|ES\|NL)$` |
| Customer 360 | `lifetime_value` | non-negative decimal |
| Product ID | `product_id` | `^2[0-9]{3}$` |
| Product Category | `category` | `^(Electronics\|Grocery\|Home\|Office\|Outdoor)$` |
| Unit Price | `unit_price` | non-negative decimal ≤ 99999.99 |
| Order ID | `order_id` | `^3[0-9]{3}$` |
| Order Status | `status` | `^(completed\|returned\|pending\|cancelled)$` |
| Order Quantity | `quantity` | `^[1-9][0-9]?$` |
| Net Amount | `net_amount` | non-negative decimal |
| Daily Sales | `order_date` | ISO 8601 date `YYYY-MM-DD` |
| Net Revenue | `net_revenue` / `total_revenue` | non-negative decimal |
| Category Performance | `total_orders` / `order_count` | non-negative integer |

## After publishing — govern the Gold assets

Run metadata enrichment on the Gold tables in project `ibmas-ingest-demo`
(`2d2415ea-71b5-4215-a7b6-b32a4889611e`) so the column-name filters automatically assign
data classes to columns:

- `gold_daily_sales`
- `gold_category_performance`
- `gold_customer_360`
