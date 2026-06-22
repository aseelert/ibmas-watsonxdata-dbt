#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  apply_openmetadata_governance.py — attach glossary, classifications, and
#                                     descriptions to OpenMetadata tables
#
#  Location  : scripts/apply_openmetadata_governance.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Apply the demo glossary and classifications to OpenMetadata.

The OpenMetadata dbt ingestion imports the dbt model graph and test results.
This script adds the governance labels that are intentionally separate from
dbt's build logic: a small business glossary, medallion-layer classification,
domain tags, and a tag that records whether the table entities came from live
Presto metadata or the offline dbt catalog seed.

Run after ``openmetadata/ingestion/run-ingestion.sh`` has created/updated table
entities. The script is idempotent and safe to re-run. By default it warns and
continues if a non-critical OpenMetadata API call is not supported by the local
server version; pass ``--strict`` to make those warnings fatal.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

import requests

ROOT = Path(__file__).resolve().parents[1]
OM_BASE = os.getenv("OM_BASE", "http://localhost:8585").rstrip("/")
GET_TOKEN = ROOT / "openmetadata" / "ingestion" / "get_om_token.py"
HTTP_TIMEOUT = 30

GLOSSARY = "MedallionGlossary"
LAYER_CLASSIFICATION = "MedallionLayer"
DOMAIN_CLASSIFICATION = "DemoDataDomain"
MODE_CLASSIFICATION = "MetadataIngestionMode"

GLOSSARY_TERMS = {
    "RawLanding": "Source-shaped landing tables loaded by dbt seed before any medallion transformation.",
    "BronzeLayer": "Managed copy of raw data with ingest metadata added.",
    "SilverLayer": "Clean, typed, conformed data with data quality rules and joins applied.",
    "GoldLayer": "Business-ready marts optimized for reporting and downstream consumption.",
    "Customer": "A person or account represented in the commerce demo.",
    "Product": "A sellable item in the product catalog.",
    "Order": "A customer purchase transaction header.",
    "OrderItem": "A line item that links an order to a product and quantity.",
    "RevenueMetric": "A governed sales amount derived from quantity, price, and discount.",
    "Customer360": "A customer-level analytic record combining profile attributes and order metrics.",
}

CLASSIFICATIONS = {
    LAYER_CLASSIFICATION: {
        "description": "Auto-applied medallion layer labels for dbt tables.",
        "tags": {
            "Raw": "Landing tables loaded by dbt seed.",
            "Bronze": "Managed raw copy with ingestion metadata.",
            "Silver": "Cleaned and conformed data.",
            "Gold": "Business-ready mart data.",
        },
    },
    DOMAIN_CLASSIFICATION: {
        "description": "Business-domain labels auto-applied from table and column names.",
        "tags": {
            "Customer": "Customer profile, identifier, or customer-level metric.",
            "Product": "Product catalog attribute or product identifier.",
            "Order": "Order header, order line, or order status field.",
            "FinancialMetric": "Money, price, discount, revenue, or lifetime value field.",
            "OperationalMetadata": "Ingestion or transformation audit metadata.",
            "PII": "Personally identifiable customer data in the demo dataset.",
        },
    },
    MODE_CLASSIFICATION: {
        "description": "Records whether OpenMetadata table entities came from live Presto or offline dbt metadata.",
        "tags": {
            "OnlinePresto": "Table entity discovered through a live Presto metadata ingestion.",
            "OfflineDbtArtifacts": "Table entity seeded from staged dbt catalog artifacts.",
        },
    },
}

TABLE_DESCRIPTIONS = {
    "raw_customers": "Raw customer seed table loaded from seeds/raw_customers.csv.",
    "raw_products": "Raw product seed table loaded from seeds/raw_products.csv.",
    "raw_orders": "Raw order header seed table loaded from seeds/raw_orders.csv.",
    "raw_order_items": "Raw order line seed table loaded from seeds/raw_order_items.csv.",
    "bronze_customers": "Bronze customer records with ingestion timestamp, source file, and batch id.",
    "bronze_products": "Bronze product records with ingestion timestamp, source file, and batch id.",
    "bronze_orders": "Bronze order headers with ingestion timestamp, source file, and batch id.",
    "bronze_order_items": "Bronze order lines with ingestion timestamp, source file, and batch id.",
    "silver_customers": "Clean customer dimension with normalized email, country, and signup date.",
    "silver_products": "Clean product dimension with typed product price and category.",
    "silver_orders": "Clean order header fact with typed timestamp, order date, status, and payment method.",
    "silver_order_items": "Clean order line fact with typed quantity and discount percentage.",
    "silver_sales_enriched": "Conformed order-line fact joining orders, customers, products, and order items.",
    "time_spine_daily": "Daily calendar spine for 2026 used by semantic model validation.",
    "gold_daily_sales": "Gold daily sales KPI mart by order date and product category.",
    "gold_category_performance": "Gold product-category performance view derived from daily sales.",
    "gold_customer_360": "Gold customer-level analytics view with lifetime order and revenue metrics.",
}

COLUMN_DESCRIPTIONS = {
    "customer_id": "Stable customer identifier.",
    "first_name": "Customer given name.",
    "last_name": "Customer family name.",
    "email": "Customer email address.",
    "signup_date": "Date the customer signed up.",
    "country": "Customer country code.",
    "customer_country": "Customer country code carried into the enriched sales fact.",
    "product_id": "Stable product identifier.",
    "product_name": "Product display name.",
    "category": "Product category used for merchandising and reporting.",
    "unit_price": "Product unit price before discounts.",
    "order_id": "Stable order header identifier.",
    "order_item_id": "Stable order line identifier.",
    "order_ts": "Timestamp when the order was placed.",
    "order_date": "Calendar date derived from the order timestamp.",
    "status": "Normalized order lifecycle status.",
    "payment_method": "Payment method recorded on the order.",
    "quantity": "Number of units purchased on the order line.",
    "discount_pct": "Discount percentage applied to the order line.",
    "gross_amount": "Line amount before discount: quantity multiplied by unit price.",
    "net_amount": "Line amount after discount.",
    "order_count": "Count of completed orders in the reporting grain.",
    "units_sold": "Total units sold in the reporting grain.",
    "net_revenue": "Net revenue for completed orders in the reporting grain.",
    "total_orders": "Total number of orders for the product category.",
    "total_units": "Total number of units sold for the product category.",
    "total_revenue": "Total net revenue for the product category.",
    "avg_revenue_per_unit": "Average net revenue per unit sold.",
    "completed_orders": "Customer's distinct completed order count.",
    "returned_orders": "Customer's distinct returned order count.",
    "pending_orders": "Customer's distinct pending order count.",
    "cancelled_orders": "Customer's distinct cancelled order count.",
    "lifetime_value": "Customer lifetime net revenue from completed orders.",
    "last_completed_order_ts": "Most recent completed order timestamp for the customer.",
    "last_activity_ts": "Most recent order timestamp for the customer across all statuses.",
    "date_day": "One calendar day in the 2026 time spine.",
    "_ingested_at": "Timestamp when the bronze row was created.",
    "_ingested_by": "Loader name that produced the bronze row.",
    "_source_file": "Source CSV file captured for audit.",
    "_ingest_batch_id": "Batch identifier captured during ingestion.",
    "transformed_at": "Timestamp when the silver transformation created the row.",
}


def _mint_token() -> str:
    try:
        out = subprocess.run(
            [sys.executable, str(GET_TOKEN)],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stderr or "")
        raise SystemExit("Could not mint an OpenMetadata token.")
    token = out.stdout.strip()
    if not token:
        raise SystemExit("get_om_token.py returned an empty token.")
    return token


class OpenMetadataClient:
    def __init__(self, token: str, strict: bool) -> None:
        self.strict = strict
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        )

    def request(self, method: str, path: str, **kwargs) -> requests.Response | None:
        url = f"{OM_BASE}/api/v1/{path.lstrip('/')}"
        try:
            resp = self.session.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
        except requests.RequestException as exc:
            self._warn_or_exit(f"{method} /{path} failed: {exc}")
            return None
        if resp.status_code >= 400:
            self._warn_or_exit(
                f"{method} /{path} failed ({resp.status_code}): {resp.text[:300]}"
            )
            return None
        return resp

    def _warn_or_exit(self, message: str) -> None:
        if self.strict:
            raise SystemExit(message)
        print(f"[WARN] {message}", file=sys.stderr)


def _tag_label(tag_fqn: str, source: str) -> dict:
    return {
        "tagFQN": tag_fqn,
        "source": source,
        "labelType": "Manual",
        "state": "Confirmed",
    }


def _layer_for_schema(schema: str) -> str:
    if schema.endswith("_raw"):
        return "Raw"
    if schema.endswith("_bronze"):
        return "Bronze"
    if schema.endswith("_silver"):
        return "Silver"
    if schema.endswith("_gold"):
        return "Gold"
    return "Raw"


def _domain_tags_for_name(name: str) -> list[str]:
    tags: list[str] = []
    if "customer" in name or name in {"first_name", "last_name", "email", "country", "signup_date"}:
        tags.append("Customer")
    if "product" in name or name in {"category", "unit_price"}:
        tags.append("Product")
    if "order" in name or name in {"status", "payment_method", "quantity", "discount_pct"}:
        tags.append("Order")
    if any(token in name for token in ("price", "amount", "revenue", "value", "discount")):
        tags.append("FinancialMetric")
    if name == "email":
        tags.append("PII")
    if name.startswith("_") or name == "transformed_at":
        tags.append("OperationalMetadata")
    return sorted(set(tags))


def _glossary_terms_for_table(table: str, layer: str) -> list[str]:
    terms = {
        "Raw": ["RawLanding"],
        "Bronze": ["BronzeLayer"],
        "Silver": ["SilverLayer"],
        "Gold": ["GoldLayer"],
    }[layer]
    if "customer_360" in table:
        terms.append("Customer360")
    elif "customer" in table:
        terms.append("Customer")
    if "product" in table or "category" in table:
        terms.append("Product")
    if "order" in table or "sales" in table:
        terms.append("Order")
    if "sales" in table or "performance" in table:
        terms.append("RevenueMetric")
    return sorted(set(terms))


def _load_catalog(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"catalog.json not found at {path}")
    return json.loads(path.read_text())


def _create_governance_entities(client: OpenMetadataClient) -> None:
    client.request(
        "PUT",
        "glossaries",
        json={
            "name": GLOSSARY,
            "displayName": "Medallion Demo Glossary",
            "description": "Business glossary for the watsonx.data dbt medallion demo.",
        },
    )
    for name, description in GLOSSARY_TERMS.items():
        client.request(
            "PUT",
            "glossaryTerms",
            json={
                "name": name,
                "displayName": name.replace("Layer", " Layer"),
                "glossary": GLOSSARY,
                "description": description,
            },
        )

    for classification, spec in CLASSIFICATIONS.items():
        client.request(
            "PUT",
            "classifications",
            json={
                "name": classification,
                "description": spec["description"],
                "autoClassificationConfig": {
                    "enabled": True,
                    "conflictResolution": "most_specific",
                    "minimumConfidence": 0.75,
                    "requireExplicitMatch": False,
                },
            },
        )
        for tag, description in spec["tags"].items():
            client.request(
                "PUT",
                "tags",
                json={
                    "classification": classification,
                    "name": tag,
                    "description": description,
                    "autoClassificationEnabled": True,
                    "autoClassificationPriority": 50,
                },
            )


def _merge_tags(existing: list[dict] | None, wanted: list[dict]) -> list[dict]:
    by_fqn = {tag.get("tagFQN"): tag for tag in existing or [] if tag.get("tagFQN")}
    for tag in wanted:
        by_fqn[tag["tagFQN"]] = tag
    return list(by_fqn.values())


def _govern_table(client: OpenMetadataClient, service: str, mode: str, node: dict) -> bool:
    meta = node["metadata"]
    db = meta["database"]
    schema = meta["schema"]
    table = meta["name"]
    layer = _layer_for_schema(schema)
    table_fqn = f"{service}.{db}.{schema}.{table}"
    encoded = quote(table_fqn, safe="")

    resp = client.request("GET", f"tables/name/{encoded}?fields=columns,tags")
    if resp is None or resp.status_code == 404:
        print(f"[skip] {table_fqn} is not present in OpenMetadata")
        return False

    current = resp.json()
    table_tags = [
        _tag_label(f"{LAYER_CLASSIFICATION}.{layer}", "Classification"),
        _tag_label(
            f"{MODE_CLASSIFICATION}.{'OnlinePresto' if mode == 'online' else 'OfflineDbtArtifacts'}",
            "Classification",
        ),
    ]
    table_tags.extend(
        _tag_label(f"{GLOSSARY}.{term}", "Glossary")
        for term in _glossary_terms_for_table(table, layer)
    )

    current_columns = {
        column["name"]: column for column in current.get("columns", []) if column.get("name")
    }
    columns = []
    for column in node.get("columns", {}).values():
        name = column["name"]
        existing = dict(current_columns.get(name, {"name": name}))
        existing["description"] = existing.get("description") or COLUMN_DESCRIPTIONS.get(name, "")
        wanted_tags = [
            _tag_label(f"{DOMAIN_CLASSIFICATION}.{tag}", "Classification")
            for tag in _domain_tags_for_name(name)
        ]
        if name in {"net_revenue", "total_revenue", "avg_revenue_per_unit", "lifetime_value", "gross_amount", "net_amount"}:
            wanted_tags.append(_tag_label(f"{GLOSSARY}.RevenueMetric", "Glossary"))
        existing["tags"] = _merge_tags(existing.get("tags"), wanted_tags)
        columns.append(existing)

    payload = {
        "name": table,
        "databaseSchema": f"{service}.{db}.{schema}",
        "description": current.get("description") or TABLE_DESCRIPTIONS.get(table, ""),
        "columns": columns,
        "tags": _merge_tags(current.get("tags"), table_tags),
    }
    if current.get("tableType"):
        payload["tableType"] = current["tableType"]

    if client.request("PUT", "tables", json=payload) is None:
        return False
    print(f"[governed] {table_fqn} ({mode})")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply glossary terms, classifications, and descriptions to OpenMetadata tables."
    )
    parser.add_argument(
        "--service",
        default=os.getenv("WXD_OM_SERVICE", "watsonxdata-presto"),
        help="OpenMetadata database service name.",
    )
    parser.add_argument(
        "--catalog-file",
        help="Path to staged dbt catalog.json.",
    )
    parser.add_argument(
        "--mode",
        choices=["online", "offline"],
        default=os.getenv("WXD_OM_INGESTION_MODE", "offline"),
        help="Classify this run as live Presto metadata or offline dbt metadata.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail on non-critical OpenMetadata API errors instead of warning.",
    )
    args = parser.parse_args()

    artifact_dir = os.getenv("WXD_DBT_ARTIFACT_DIR", "openmetadata/dbt-artifacts")
    catalog_path = (
        Path(args.catalog_file).expanduser()
        if args.catalog_file
        else ROOT / artifact_dir / "catalog.json"
    )
    if not catalog_path.is_absolute():
        catalog_path = ROOT / catalog_path

    catalog = _load_catalog(catalog_path)
    nodes = catalog.get("nodes", {})
    if not nodes:
        raise SystemExit("catalog.json has no nodes to govern.")

    client = OpenMetadataClient(_mint_token(), strict=args.strict)
    _create_governance_entities(client)

    governed = 0
    for _, node in sorted(nodes.items(), key=lambda kv: kv[1]["metadata"]["name"]):
        if _govern_table(client, args.service, args.mode, node):
            governed += 1

    print(f"Applied OpenMetadata governance to {governed} table(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
