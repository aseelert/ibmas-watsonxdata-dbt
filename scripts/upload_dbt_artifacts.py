#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  upload_dbt_artifacts.py — push staged dbt artifacts to S3 object storage for OpenMetadata
#
#  Location  : scripts/upload_dbt_artifacts.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Uploads staged dbt artifacts to
#      S3-compatible object storage for OpenMetadata.
# -----------------------------------------------------------------------------
"""Upload dbt artifacts to S3-compatible storage for OpenMetadata.

This is the SECOND half of the OpenMetadata lineage workflow. It takes the dbt
artifacts already staged on disk by
``scripts/prepare_openmetadata_dbt_artifacts.py`` (``manifest.json`` plus the
optional ``catalog.json`` and ``run_results.json``) and uploads them to the
S3-compatible object store (MinIO / watsonx.data bucket) under a stable prefix.
OpenMetadata's dbt ingestion connector then reads those objects from S3 to
build dbt model/table lineage and column-level metadata for the medallion demo.

WHAT it does:
 - Resolves the local staging directory and verifies the REQUIRED artifact
   (``manifest.json``) is present; warns about any missing OPTIONAL artifacts
   but still uploads whatever is available.
 - Reuses the object-store plumbing from ``upload_spark_assets`` (shared
   ``ROOT``, ``_env``, ``_maybe_start_port_forward`` and
   ``_object_store_credentials`` helpers) so endpoint resolution, optional
   ``kubectl`` port-forwarding and credential discovery behave identically to
   the Spark-asset uploader.
 - Builds a boto3 S3 client with capped connect/read timeouts (10s / 60s) and
   bounded retries so a stalled MinIO cannot hang the upload, then uploads each
   present artifact and prints the resulting ``s3://`` paths.

WHEN to run it: AFTER ``scripts/prepare_openmetadata_dbt_artifacts.py`` has
staged the artifacts locally, and before (or as part of) configuring the
OpenMetadata dbt ingestion to point at the printed S3 paths.

ENV VARS read:
 - ``WXD_DBT_ARTIFACT_DIR`` — local staging directory to read from
   (default: ``openmetadata/dbt-artifacts``, resolved relative to repo root).
 - ``WXD_DBT_ARTIFACT_BUCKET`` — target bucket; falls back to
   ``WXD_SPARK_ASSET_BUCKET`` (default ``iceberg-bucket``).
 - ``WXD_DBT_ARTIFACT_PREFIX`` — object-key prefix; defaults to
   ``openmetadata/dbt-artifacts/<WXD_SCHEMA>`` (``WXD_SCHEMA`` default
   ``dbt_demo``).
 - ``WXD_OBJECT_STORE_ENDPOINT`` — S3 endpoint URL (required).
 - ``WXD_OBJECT_STORE_REGION`` — S3 region (default ``us-east-1``).
 - ``WXD_OBJECT_STORE_SSL_VERIFY`` — ``true``/``false`` TLS verification
   toggle (default ``false``).
 - Plus the credential/port-forward env vars consumed by the imported
   ``upload_spark_assets`` helpers. ``.env`` is loaded via python-dotenv when
   available.

PREREQUISITES: ``boto3`` installed (``pip install -r requirements.txt``),
reachable object-store endpoint (or a working ``kubectl`` context if a
port-forward is needed), and the staged artifacts on disk.

USAGE example:
 - ``python3 scripts/upload_dbt_artifacts.py``

SIDE EFFECTS + EXIT: writes objects into the S3 bucket, may start and then
terminate a port-forward subprocess, prints the uploaded ``s3://`` paths, and
returns exit code 0 on success. Raises ``SystemExit`` (non-zero) when ``boto3``
is missing or the required ``manifest.json`` is not staged.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from upload_spark_assets import (
    ROOT,
    _env,
    _maybe_start_port_forward,
    _object_store_credentials,
)


REQUIRED_ARTIFACTS = ["manifest.json"]
OPTIONAL_ARTIFACTS = ["catalog.json", "run_results.json"]
ARTIFACTS = REQUIRED_ARTIFACTS + OPTIONAL_ARTIFACTS


def _artifact_dir() -> Path:
    value = os.getenv("WXD_DBT_ARTIFACT_DIR", "openmetadata/dbt-artifacts")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path


def main() -> int:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    print("== upload OpenMetadata dbt artifacts to object storage ==")

    try:
        import boto3
        import botocore.config
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'boto3'. Install dependencies with: python -m pip install -r requirements.txt"
        ) from exc

    artifact_dir = _artifact_dir()
    missing_required = [
        name for name in REQUIRED_ARTIFACTS if not (artifact_dir / name).exists()
    ]
    missing_optional = [
        name for name in OPTIONAL_ARTIFACTS if not (artifact_dir / name).exists()
    ]
    if missing_required:
        raise SystemExit(
            "Missing required staged dbt artifacts: "
            + ", ".join(missing_required)
            + ". Run scripts/prepare_openmetadata_dbt_artifacts.py first."
        )
    if missing_optional:
        print(
            "Optional staged artifacts missing: "
            + ", ".join(missing_optional)
            + ". Uploading available artifacts."
        )

    endpoint = _env("WXD_OBJECT_STORE_ENDPOINT")
    port_forward = _maybe_start_port_forward(endpoint)
    access_key, secret_key = _object_store_credentials()
    bucket = os.getenv("WXD_DBT_ARTIFACT_BUCKET") or _env("WXD_SPARK_ASSET_BUCKET", "iceberg-bucket")
    prefix = os.getenv(
        "WXD_DBT_ARTIFACT_PREFIX",
        f"openmetadata/dbt-artifacts/{os.getenv('WXD_SCHEMA', 'dbt_demo')}",
    ).strip("/")
    region = os.getenv("WXD_OBJECT_STORE_REGION", "us-east-1")
    verify_value = os.getenv("WXD_OBJECT_STORE_SSL_VERIFY", "false").lower()
    verify = verify_value not in {"0", "false", "no"}

    print(f"Object store endpoint: {endpoint}")
    print(f"Target artifact prefix: s3://{bucket}/{prefix}")

    # Cap S3 connect/read so a stalled MinIO can't hang the upload indefinitely.
    s3_config = botocore.config.Config(
        connect_timeout=10,
        read_timeout=60,
        retries={"max_attempts": 2},
    )

    try:
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
        for name in ARTIFACTS:
            if not (artifact_dir / name).exists():
                continue
            key = f"{prefix}/{name}"
            print(f"uploading {name} -> s3://{bucket}/{key} ...")
            s3.upload_file(str(artifact_dir / name), bucket, key)
            print(f"[OK] uploaded s3://{bucket}/{key}")

        print()
        print("OpenMetadata S3 artifact paths:")
        print(f"  Manifest: s3://{bucket}/{prefix}/manifest.json")
        if (artifact_dir / "catalog.json").exists():
            print(f"  Catalog: s3://{bucket}/{prefix}/catalog.json")
        if (artifact_dir / "run_results.json").exists():
            print(f"  Run Results: s3://{bucket}/{prefix}/run_results.json")
    finally:
        if port_forward is not None:
            port_forward.terminate()
    return 0


if __name__ == "__main__":
    # Top-level safety net: known errors raise SystemExit with a clear message and
    # are passed through; anything unexpected is logged with context and exits 1.
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[ERROR] interrupted by user", file=sys.stderr)
        sys.exit(130)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 — log the unexpected failure, then exit non-zero
        import traceback
        print(f"[ERROR] unexpected failure: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
