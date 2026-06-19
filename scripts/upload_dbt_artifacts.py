#!/usr/bin/env python3
"""Upload dbt artifacts to S3-compatible storage for OpenMetadata."""

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

    try:
        import boto3
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

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            verify=verify,
        )
        for name in ARTIFACTS:
            if not (artifact_dir / name).exists():
                continue
            key = f"{prefix}/{name}"
            s3.upload_file(str(artifact_dir / name), bucket, key)
            print(f"uploaded s3://{bucket}/{key}")

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
    sys.exit(main())
