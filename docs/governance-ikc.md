# IBM Knowledge Catalog — Governance Layer

The Retail Medallion Lakehouse ships a complete governance layer in **IBM Knowledge Catalog
(watsonx.data Intelligence)**. Every column that flows through Bronze → Silver → Gold is
described by a business term, classified, matched by a custom regex data class, and —
where PII applies — masked by a data protection rule.

All source files live in `governance/ikc/` in this repository and are imported in four sequential steps described below.

---

## Governance hierarchy

```
Retail Medallion Lakehouse          ← top-level category
├── Customer                        ← 4 business terms + data classes
├── Product                         ← 3 business terms + data classes
├── Order                           ← 4 business terms + data classes
├── Gold Sales                      ← 3 business terms + data classes
└── Data Governance
    ├── Business Data  (classification — applied to all 14 terms)
    └── Personal Data  (classification — Customer ID + Customer Email only)
```

Source file: see `governance/ikc/` in the repository root

---

## Classifications

Two custom classifications live under the `Data Governance` sub-category.

| Classification | Parent | Applied to |
|---------------|--------|------------|
| Business Data | — | All 14 data classes |
| Personal Data | Business Data | RML Customer ID, RML Customer Email |

Source file: see `governance/ikc/` in the repository root

---

## Business terms

Fourteen terms describe every governed concept in the medallion pipeline.
All terms are published under the `Retail Medallion Lakehouse` category hierarchy.

| Term | Category | Source column | Description |
|------|----------|--------------|-------------|
| Customer ID | Customer | `customer_id` | 4-digit integer PK (1001–1050). Join key from orders back to customer. |
| Customer Email | Customer | `email` | RFC 5321 email. Only direct PII in the seed — masked by data protection rule. |
| Customer Country | Customer | `country` | ISO 3166-1 alpha-2 code (US, GB, DE, FR, IT, ES, NL). |
| Customer 360 | Customer | `lifetime_value` | Gold mart: cumulative spend + order counts per customer. |
| Product ID | Product | `product_id` | 4-digit integer PK (2001–2020). Join key from order items to products. |
| Product Category | Product | `category` | Controlled domain: Electronics / Grocery / Home / Office / Outdoor. |
| Unit Price | Product | `unit_price` | List price before discount; drives `net_amount` in Silver. |
| Order ID | Order | `order_id` | 4-digit integer PK (3001–3500). One order → many line items. |
| Order Status | Order | `status` | Lifecycle code: completed / returned / pending / cancelled. |
| Order Quantity | Order | `quantity` | Units per line item (1–99); rolls into `units_sold` in Gold. |
| Net Amount | Order | `net_amount` | `unit_price × quantity × (1 − discount_pct)` — atomic revenue measure. |
| Daily Sales | Gold Sales | `order_date` | Gold PARQUET table grain: one row per order_date × category. |
| Net Revenue | Gold Sales | `net_revenue` | Sum of `net_amount` for completed orders; primary Gold KPI. |
| Category Performance | Gold Sales | `total_orders` | Gold mart: total_orders / units / revenue per product category. |

Source file: see `governance/ikc/` in the repository root

---

## Data classes (custom regex, prefix `RML`)

Each data class contains a `RegularExpressionClassifier` (value-level pattern) and a
`ColumnNameFilter` (column-name regex). Running **metadata enrichment** on a Gold table
auto-assigns the matching data class to each column without manual annotation.

All names carry the `RML ` prefix to avoid conflicts with the hundreds of built-in IBM
data classes already published in the catalog (e.g. `Customer ID` and `Customer Country`
exist as system data classes — our custom versions are `RML Customer ID` / `RML Customer Country`).

| Data class | Column matched | Value regex | Classification |
|------------|---------------|-------------|----------------|
| RML Customer ID | `customer_id` | `^[0-9]{4}$` | Business Data, Personal Data |
| RML Customer Email | `email` | RFC 5321 email | Business Data, Personal Data |
| RML Customer Country | `country` | `^(US\|GB\|DE\|FR\|IT\|ES\|NL)$` | Business Data |
| RML Customer 360 | `lifetime_value` | Non-negative decimal | Business Data |
| RML Product ID | `product_id` | `^2[0-9]{3}$` | Business Data |
| RML Product Category | `category` | `^(Electronics\|Grocery\|Home\|Office\|Outdoor)$` | Business Data |
| RML Unit Price | `unit_price` | Non-negative decimal ≤ 99999.99 | Business Data |
| RML Order ID | `order_id` | `^3[0-9]{3}$` | Business Data |
| RML Order Status | `status` | `^(completed\|returned\|pending\|cancelled)$` | Business Data |
| RML Order Quantity | `quantity` | `^[1-9][0-9]?$` | Business Data |
| RML Net Amount | `net_amount` | Non-negative decimal | Business Data |
| RML Daily Sales | `order_date` | `YYYY-MM-DD` ISO 8601 | Business Data |
| RML Net Revenue | `net_revenue` / `total_revenue` | Non-negative decimal | Business Data |
| RML Category Performance | `total_orders` / `order_count` | Non-negative integer | Business Data |

Source file: see `governance/ikc/` in the repository root

---

## Data protection rule

| Field | Value |
|-------|-------|
| Name | `RML — Mask Customer Email (PII)` |
| Trigger | Column name `LIKE email` |
| Action | Redact — replace value with masked output |
| State | Active |
| Scope | All assets in `ibmas-catalog` that contain an `email` column |

Source file: see `governance/ikc/` in the repository root

!!! note "Policy service on CPD 5.3.4"
    The data protection rule must be created via the **IKC UI** (Governance → Data protection
    rules → Add rule). The policy service REST endpoint returns HTTP 405 on this CPD version
    when called via MCP or REST directly.

---

## Impact on your data

The table below maps every governed column across all medallion layers.
🔒 marks columns masked by the active data protection rule.

| Layer | Table | Column | Business Term | Data Class | PII? | Masked? |
|-------|-------|--------|---------------|------------|------|---------|
| Bronze / Silver | `*_customers` | `customer_id` | Customer ID | RML Customer ID | ✅ | — |
| Bronze / Silver | `*_customers` | `email` | Customer Email | RML Customer Email | ✅ | 🔒 Yes |
| Bronze / Silver | `*_customers` | `country` | Customer Country | RML Customer Country | — | — |
| Bronze / Silver | `*_products` | `product_id` | Product ID | RML Product ID | — | — |
| Bronze / Silver | `*_products` | `category` | Product Category | RML Product Category | — | — |
| Bronze / Silver | `*_products` | `unit_price` | Unit Price | RML Unit Price | — | — |
| Bronze / Silver | `*_orders` | `order_id` | Order ID | RML Order ID | — | — |
| Bronze / Silver | `*_orders` | `status` | Order Status | RML Order Status | — | — |
| Silver | `silver_order_items` | `quantity` | Order Quantity | RML Order Quantity | — | — |
| Silver | `silver_order_items` | `net_amount` | Net Amount | RML Net Amount | — | — |
| Gold | `gold_customer_360` | `customer_id` | Customer ID | RML Customer ID | ✅ | — |
| Gold | `gold_customer_360` | `email` | Customer Email | RML Customer Email | ✅ | 🔒 Yes |
| Gold | `gold_customer_360` | `lifetime_value` | Customer 360 | RML Customer 360 | — | — |
| Gold | `gold_daily_sales` | `order_date` | Daily Sales | RML Daily Sales | — | — |
| Gold | `gold_daily_sales` | `net_revenue` | Net Revenue | RML Net Revenue | — | — |
| Gold | `gold_category_performance` | `total_orders` | Category Performance | RML Category Performance | — | — |

!!! tip "Masking propagates through all layers"
    The rule triggers on the column name `email` — it fires uniformly across the Bronze raw
    table, Silver cleaned table, and Gold `gold_customer_360` view. A user querying any layer
    sees the masked value, not the raw address.

---

## Staged import order

IKC import has strict dependency rules. The four CSV files must be imported in this order,
with a **publish step after each one** (UI workflow inbox) before moving to the next.

```
Step 1  governance/ikc/01_categories.csv      → Governance → Categories → Import
        ↳ Publish workflow tasks

Step 2  governance/ikc/03_classifications.csv  → Governance → Classifications → Import
        ↳ Publish workflow tasks
        (Business Data + Personal Data are now live — can be referenced)

Step 3  governance/ikc/02_business_terms.csv   → Governance → Business terms → Import (merge: all)
        ↳ Publish workflow tasks
        (14 terms are now live — data classes can reference them)

Step 4  governance/ikc/04_data_classes.csv     → Governance → Data classes → Import (merge: all)
        ↳ Publish workflow tasks
```

!!! warning "Known pitfalls"
    - **`GIM00015E`** — A referenced artifact (classification, term) is in DRAFT or not yet imported.
      Publish the dependency first.
    - **`WKCBG5003E: entity attributes are not unique`** — A data class name conflicts with a
      published system data class. Prefix all custom DC names with `RML ` to avoid collisions.
    - **`GIM00006E: artifact type cannot be imported`** — Wrong `Artifact Type` in the CSV.
      Rules use `rule`, not `governance_rule`.
    - **Related Terms need full `>>` paths** in data class CSVs — `Retail Medallion Lakehouse >> Customer >> Customer ID`,
      not just `Customer ID`.
    - **`>>` is the only valid category separator** — not `/` (the MCP schema is wrong on this).
