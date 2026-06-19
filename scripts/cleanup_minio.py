#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  cleanup_minio.py — scoped delete of the demo's MinIO/S3 prefixes for a clean rerun
#
#  Location  : scripts/cleanup_minio.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Delete the demo's files from MinIO/S3 so a rerun starts 100% clean.

WHAT / WHY
    This is a *scoped* deletion. It only removes the demo's own prefixes inside
    ``WXD_SPARK_ASSET_BUCKET`` (default ``iceberg-bucket``):

      * the medallion schema folders that Iceberg writes at the bucket root
        (``dbt_demo_*``, ``spark_demo_*``, the cpdctl raw schema) — i.e. the actual
        table data + metadata files,
      * the ``spark_demo`` asset prefix (the uploaded PySpark app and raw CSVs),
      * the ``openmetadata/dbt-artifacts`` prefix (published dbt manifest/catalog).

    It NEVER empties the whole bucket — only these known demo prefixes. After a real
    delete it VERIFIES each prefix is empty (no relics/orphans) and lists the
    surviving top-level folders so you can confirm the bucket (and any non-demo
    data) is intact.

WHEN TO RUN IT
    Pairs with ``scripts/cleanup_watsonxdata.py`` (which DROPs the Presto/Iceberg
    schemas). Recommended order: drop the schemas first, then run this to remove any
    files the drop left behind. The all-in-one ``scripts/reset_demo.sh`` does both.

ENV VARS (read here)
    WXD_OBJECT_STORE_ENDPOINT       — S3 endpoint URL (required).
    WXD_SPARK_ASSET_BUCKET          — bucket to clean (default ``iceberg-bucket``).
    WXD_OBJECT_STORE_REGION         — S3 region (default ``us-east-1``).
    WXD_OBJECT_STORE_SSL_VERIFY     — TLS verify toggle (default ``false``).
    WXD_SCHEMA / WXD_SPARK_SCHEMA   — base names used to derive the medallion
                                      schema folders.
    WXD_RAW_SCHEMA, WXD_BRONZE_SCHEMA, WXD_SILVER_SCHEMA, WXD_GOLD_SCHEMA,
    WXD_SPARK_BRONZE_SCHEMA, WXD_SPARK_SILVER_SCHEMA, WXD_SPARK_GOLD_SCHEMA,
    WXD_INGEST_SCHEMA               — per-layer schema-folder overrides.
    WXD_SPARK_ASSET_PREFIX          — uploaded-asset prefix (default ``spark_demo``).
    Plus all credential/port-forward vars consumed by the shared helpers imported
    from ``upload_spark_assets`` (WXD_OBJECT_STORE_ACCESS_KEY/_SECRET_KEY,
    WXD_OPENSHIFT_NAMESPACE, WXD_OBJECT_STORE_SECRET_NAME, etc.).

PREREQUISITES
    On this cluster MinIO has no external Route, so the object store is reached via
    an ``oc`` port-forward — exactly like ``scripts/upload_spark_assets.py`` (whose
    ``_maybe_start_port_forward`` + ``_object_store_credentials`` are reused so the
    two scripts behave identically). You must be logged in with ``oc`` (the
    credentials are read from the ``ibm-lh-minio-secret`` OpenShift secret unless
    ``WXD_OBJECT_STORE_ACCESS_KEY``/``_SECRET_KEY`` are set). Requires ``boto3``.

USAGE
    python scripts/cleanup_minio.py --dry-run   # list what WOULD be deleted
    python scripts/cleanup_minio.py             # actually delete

SIDE EFFECTS / EXIT
    With ``--dry-run`` nothing is deleted (objects are only counted). Without it,
    every object under each demo prefix is removed (batched by 1000 per
    ``delete_objects`` call). May spawn (and always tears down) an ``oc``
    port-forward. Exits non-zero with a clear message on missing env/creds or a
    missing ``boto3``; otherwise returns 0.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Reuse the proven oc port-forward + credential helpers from the uploader so the
# two scripts behave identically (same secret, same port-forward, same endpoint).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from upload_spark_assets import (  # noqa: E402
    _maybe_start_port_forward,
    _object_store_credentials,
)


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def _demo_prefixes() -> list[str]:
    """Return the exact S3 prefixes the demo owns (always trailing-slash scoped)."""
    dbt_base = os.getenv("WXD_SCHEMA", "dbt_demo")
    spark_base = os.getenv("WXD_SPARK_SCHEMA", "spark_demo")

    # Iceberg writes table data/metadata at the bucket root, one folder per schema.
    schema_names = [
        os.getenv("WXD_RAW_SCHEMA", f"{dbt_base}_raw"),
        os.getenv("WXD_BRONZE_SCHEMA", f"{dbt_base}_bronze"),
        os.getenv("WXD_SILVER_SCHEMA", f"{dbt_base}_silver"),
        os.getenv("WXD_GOLD_SCHEMA", f"{dbt_base}_gold"),
        os.getenv("WXD_SPARK_BRONZE_SCHEMA", f"{spark_base}_bronze"),
        os.getenv("WXD_SPARK_SILVER_SCHEMA", f"{spark_base}_silver"),
        os.getenv("WXD_SPARK_GOLD_SCHEMA", f"{spark_base}_gold"),
        os.getenv("WXD_INGEST_SCHEMA", f"{spark_base}_cpdctl_raw"),
    ]
    prefixes = [f"{name.strip('/')}/" for name in schema_names]

    # Uploaded Spark app + raw CSVs (e.g. spark_demo/app/..., spark_demo/raw/...).
    asset_prefix = os.getenv("WXD_SPARK_ASSET_PREFIX", "spark_demo").strip("/")
    prefixes.append(f"{asset_prefix}/")

    # Published dbt artifacts for OpenMetadata.
    prefixes.append("openmetadata/dbt-artifacts/")

    # De-duplicate while preserving order.
    seen: set[str] = set()
    ordered: list[str] = []
    for p in prefixes:
        if p not in seen:
            seen.add(p)
            ordered.append(p)
    return ordered


def _delete_prefix(s3, bucket: str, prefix: str, dry_run: bool) -> int:
    """List (and, unless dry-run, delete) every object under ``prefix``.

    Returns the number of objects matched. Paginates + batches deletes by 1000
    (the S3 ``delete_objects`` limit) so even a large table folder stays bounded.
    """
    paginator = s3.get_paginator("list_objects_v2")
    batch: list[dict[str, str]] = []
    matched = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            matched += 1
            if dry_run:
                continue
            batch.append({"Key": obj["Key"]})
            if len(batch) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                batch = []
    if batch:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
    return matched


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be deleted without deleting anything.",
    )
    args = parser.parse_args()

    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    try:
        import boto3
        import botocore.config
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'boto3'. Install with: python -m pip install -r requirements.txt"
        ) from exc

    endpoint = _env("WXD_OBJECT_STORE_ENDPOINT")
    bucket = _env("WXD_SPARK_ASSET_BUCKET", "iceberg-bucket")
    region = os.getenv("WXD_OBJECT_STORE_REGION", "us-east-1")
    verify = os.getenv("WXD_OBJECT_STORE_SSL_VERIFY", "false").lower() not in {"0", "false", "no"}
    prefixes = _demo_prefixes()

    mode = "DRY-RUN (no deletes)" if args.dry_run else "DELETE"
    print(f"MinIO cleanup [{mode}]")
    print(f"  endpoint: {endpoint}")
    print(f"  bucket:   s3://{bucket}")
    print(f"  prefixes ({len(prefixes)}):")
    for p in prefixes:
        print(f"    - s3://{bucket}/{p}")
    print()

    port_forward = _maybe_start_port_forward(endpoint)
    s3_config = botocore.config.Config(
        connect_timeout=10, read_timeout=60, retries={"max_attempts": 2}
    )
    total = 0
    try:
        access_key, secret_key = _object_store_credentials()
        print(f"Creating S3 client for {endpoint} (connect 10s / read 60s) ...")
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            verify=verify,
            config=s3_config,
        )
        for prefix in prefixes:
            n = _delete_prefix(s3, bucket, prefix, args.dry_run)
            total += n
            verb = "would delete" if args.dry_run else "deleted"
            marker = "" if n else "  (empty)"
            print(f"  {verb} {n:>5} object(s) under {prefix}{marker}")

        # After a real delete, VERIFY each demo prefix is now empty — i.e. the
        # schema/table drop + this sweep left no relics or orphan files behind.
        if not args.dry_run:
            print("\nVerifying demo prefixes are empty (no relics/orphans)...")
            residual = 0
            for prefix in prefixes:
                left = _delete_prefix(s3, bucket, prefix, dry_run=True)  # count only
                if left:
                    residual += left
                    print(f"  WARNING: {left} object(s) STILL under {prefix}")
            if residual == 0:
                print("  ✓ all demo prefixes are empty — no orphans remain.")
            else:
                print(f"  {residual} residual object(s) remain — re-run, or investigate.")

        # Show the bucket itself is preserved and what is left at the top level,
        # so you can confirm the demo folders are gone but the bucket (and any
        # non-demo data) is intact.
        try:
            resp = s3.list_objects_v2(Bucket=bucket, Delimiter="/")
            folders = sorted(cp["Prefix"] for cp in resp.get("CommonPrefixes", []))
            prefix_set = set(prefixes)
            tense = "would remain" if args.dry_run else "remain"
            stale_demo = False
            print(f"\nBucket s3://{bucket} is KEPT (never deleted).")
            print(f"Top-level folders that {tense} ({len(folders)}):")
            for f in folders:
                if f in prefix_set:
                    tag = "  <- demo (this script clears it)"
                    if not args.dry_run:
                        stale_demo = True
                elif any(p.startswith(f) and p != f for p in prefixes):
                    tag = "  <- partly demo (only a sub-folder is cleared; rest kept)"
                else:
                    tag = "  (not this demo — left untouched)"
                print(f"  s3://{bucket}/{f}{tag}")
            if stale_demo:
                print("  NOTE: a fully-demo folder still showing means residual objects — see warnings above.")
        except Exception as exc:
            print(f"  (could not list bucket top level: {exc})")
    finally:
        if port_forward is not None:
            port_forward.terminate()
            print("stopped port-forward")

    verb = "Would delete" if args.dry_run else "Deleted"
    print(f"\n{verb} {total} object(s) total across {len(prefixes)} prefix(es).")
    if args.dry_run and total:
        print("Re-run without --dry-run to actually delete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
