#!/usr/bin/env python3
"""Upload Spark demo assets to S3-compatible object storage.

This stages the PySpark application and the raw CSV files where the watsonx.data
Spark engine can read them.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


ROOT = Path(__file__).resolve().parents[1]


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def _oc_secret_value(secret_name: str, key: str, namespace: str) -> str | None:
    try:
        result = subprocess.run(
            [
                "oc",
                "get",
                "secret",
                secret_name,
                "-n",
                namespace,
                "-o",
                f"jsonpath={{.data.{key}}}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        if not result.stdout:
            return None
        decoded = subprocess.run(
            ["base64", "--decode"],
            input=result.stdout,
            check=True,
            capture_output=True,
            text=True,
        )
        return decoded.stdout
    except Exception:
        return None


def _object_store_credentials() -> tuple[str, str]:
    access_key = os.getenv("WXD_OBJECT_STORE_ACCESS_KEY")
    secret_key = os.getenv("WXD_OBJECT_STORE_SECRET_KEY")
    if access_key and secret_key:
        return access_key, secret_key

    namespace = os.getenv("WXD_OPENSHIFT_NAMESPACE", "cpd-instance")
    secret_name = os.getenv("WXD_OBJECT_STORE_SECRET_NAME", "ibm-lh-minio-secret")
    access_key_name = os.getenv("WXD_OBJECT_STORE_ACCESS_KEY_NAME", "LH_S3_ACCESS_KEY")
    secret_key_name = os.getenv("WXD_OBJECT_STORE_SECRET_KEY_NAME", "LH_S3_SECRET_KEY")

    access_key = _oc_secret_value(secret_name, access_key_name, namespace)
    secret_key = _oc_secret_value(secret_name, secret_key_name, namespace)
    if access_key and secret_key:
        return access_key, secret_key

    raise SystemExit(
        "Missing MinIO credentials. Set WXD_OBJECT_STORE_ACCESS_KEY and "
        "WXD_OBJECT_STORE_SECRET_KEY, or log in with oc so the uploader can "
        "read ibm-lh-minio-secret."
    )


def _existing_object_size(s3, bucket: str, key: str) -> int | None:
    """Return the size of an existing object, or None if it does not exist yet."""
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except Exception:
        return None


def _port_is_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _maybe_start_port_forward(endpoint: str) -> subprocess.Popen[str] | None:
    parsed = urlparse(endpoint)
    host = parsed.hostname
    port = parsed.port
    if host not in {"127.0.0.1", "localhost"} or port is None:
        return None
    if _port_is_open(host, port):
        return None
    if os.getenv("WXD_OBJECT_STORE_AUTO_PORT_FORWARD", "true").lower() in {"0", "false", "no"}:
        return None

    namespace = os.getenv("WXD_OPENSHIFT_NAMESPACE", "cpd-instance")
    service = os.getenv("WXD_OBJECT_STORE_SERVICE", "ibm-lh-lakehouse-minio-svc")
    service_port = os.getenv("WXD_OBJECT_STORE_SERVICE_PORT", "9000")
    log_path = ROOT / "logs" / "minio-port-forward.log"
    log_path.parent.mkdir(exist_ok=True)

    log_file = log_path.open("w")
    process = subprocess.Popen(
        [
            "oc",
            "-n",
            namespace,
            "port-forward",
            f"svc/{service}",
            f"{port}:{service_port}",
        ],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )

    for _ in range(30):
        if process.poll() is not None:
            raise SystemExit(
                f"oc port-forward exited early. See {log_path} for details."
            )
        if _port_is_open(host, port):
            print(f"started port-forward {host}:{port} -> {service}:{service_port}")
            return process
        time.sleep(0.5)

    process.terminate()
    raise SystemExit(
        f"Timed out waiting for {host}:{port}. See {log_path} for port-forward logs."
    )


def main() -> int:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    try:
        import boto3
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'boto3'. Install dependencies with: python -m pip install -r requirements.txt"
        ) from exc

    endpoint = _env("WXD_OBJECT_STORE_ENDPOINT")
    port_forward = _maybe_start_port_forward(endpoint)
    access_key, secret_key = _object_store_credentials()
    bucket = _env("WXD_SPARK_ASSET_BUCKET", "iceberg-bucket")
    prefix = os.getenv("WXD_SPARK_ASSET_PREFIX", "spark_demo").strip("/")
    region = os.getenv("WXD_OBJECT_STORE_REGION", "us-east-1")
    verify_value = os.getenv("WXD_OBJECT_STORE_SSL_VERIFY", "false").lower()
    verify = verify_value not in {"0", "false", "no"}

    print(f"Object store endpoint: {endpoint}")
    print(f"Target bucket/prefix: s3://{bucket}/{prefix}")

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            verify=verify,
        )

        uploads = [
            (ROOT / "spark" / "load_medallion_demo.py", f"{prefix}/app/load_medallion_demo.py"),
        ]
        uploads.extend(
            (path, f"{prefix}/raw/{path.name}")
            for path in sorted((ROOT / "seeds").glob("raw_*.csv"))
        )

        print(f"Uploading {len(uploads)} files for Spark demo (existing objects are overwritten)")
        for source, key in uploads:
            local_size = source.stat().st_size
            prior_size = _existing_object_size(s3, bucket, key)
            # PutObject is overwrite-by-key: re-running always replaces the prior object,
            # so the Spark engine reads the freshly uploaded application/CSV on every run.
            s3.upload_file(str(source), bucket, key)
            # Verify the object now in MinIO matches the local file we just sent.
            remote_size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
            if remote_size != local_size:
                raise SystemExit(
                    f"Upload verification failed for s3://{bucket}/{key}: "
                    f"local {local_size} bytes != remote {remote_size} bytes"
                )
            action = "overwrote" if prior_size is not None else "created"
            print(f"{action} s3://{bucket}/{key}  ({remote_size} bytes, verified)")

        print()
        print(f"WXD_SPARK_APPLICATION=s3a://{bucket}/{prefix}/app/load_medallion_demo.py")
        print(f"WXD_SPARK_INPUT_BASE=s3a://{bucket}/{prefix}/raw")
    finally:
        if port_forward is not None:
            port_forward.terminate()
    return 0


if __name__ == "__main__":
    sys.exit(main())
