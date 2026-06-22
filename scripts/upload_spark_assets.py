#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  upload_spark_assets.py — stage the PySpark app + raw CSVs into MinIO/S3
#
#  Location  : scripts/upload_spark_assets.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Upload Spark demo assets to S3-compatible object storage.

WHAT / WHY
    Stages the PySpark application (``spark/load_medallion_demo.py``) and the raw
    CSV seeds (``seeds/raw_*.csv``) into the object store where the watsonx.data
    Spark engine can read them. The Spark job runs *inside* the cluster and can
    only see data in MinIO/S3, so this uploader is the bridge that makes the local
    repo assets visible to the remote engine. Uploads are overwrite-by-key, so a
    re-run always replaces the prior object and each Spark run reads the freshest
    code/CSVs. Every upload is verified by comparing the local byte size against
    the ``head_object`` ContentLength returned by MinIO.

WHEN TO RUN IT
    Run this BEFORE submitting the Spark medallion job (it produces the
    ``WXD_SPARK_APPLICATION`` and ``WXD_SPARK_INPUT_BASE`` S3A paths printed at the
    end, which the submit step consumes). Its sibling cleanup script
    ``scripts/cleanup_minio.py`` re-uses this module's port-forward + credential
    helpers, so keeping them in sync matters.

ENV VARS (read here)
    WXD_OBJECT_STORE_ENDPOINT       — S3 endpoint URL (required).
    WXD_SPARK_ASSET_BUCKET          — target bucket (default ``iceberg-bucket``).
    WXD_SPARK_ASSET_PREFIX          — key prefix for assets (default ``spark_demo``).
    WXD_OBJECT_STORE_REGION         — S3 region (default ``us-east-1``).
    WXD_OBJECT_STORE_SSL_VERIFY     — TLS verify toggle (default ``false``).
    WXD_OBJECT_STORE_ACCESS_KEY /
    WXD_OBJECT_STORE_SECRET_KEY     — explicit creds; if unset they are read from
                                      the OpenShift MinIO secret via ``oc``.
    WXD_OPENSHIFT_NAMESPACE         — namespace for the secret/port-forward
                                      (default ``cpd-instance``).
    WXD_OPENSHIFT_CONTEXT           — oc kubeconfig context for all ``oc`` calls
                                      (secret read + port-forward). Unset means the
                                      current context. Pin this so a stray default
                                      context can't point the uploader at the wrong
                                      cluster.
    WXD_OBJECT_STORE_SECRET_NAME    — secret holding the creds
                                      (default ``ibm-lh-minio-secret``).
    WXD_OBJECT_STORE_ACCESS_KEY_NAME / _SECRET_KEY_NAME — keys inside that secret.
    WXD_OBJECT_STORE_AUTO_PORT_FORWARD — auto ``oc port-forward`` when the endpoint
                                      is localhost (default ``true``).
    WXD_OBJECT_STORE_SERVICE / _SERVICE_PORT — MinIO service + port to forward.

PREREQUISITES
    Either set the explicit access/secret env vars, OR be logged in with ``oc`` so
    the script can read ``ibm-lh-minio-secret``. On clusters where MinIO has no
    external Route the endpoint is localhost and an ``oc`` port-forward is started
    automatically (logged to ``minio-port-forward.log`` under the first writable
    runtime log directory). Requires ``boto3``
    (``python -m pip install -r requirements.txt``).

USAGE
    python scripts/upload_spark_assets.py

SIDE EFFECTS / EXIT
    Writes objects under ``s3://<bucket>/<prefix>/app`` and ``.../raw``; may spawn
    (and always tears down) an ``oc`` port-forward. Exits non-zero with a clear
    message on missing env/creds, a failed upload verification, or a port-forward
    timeout; otherwise returns 0.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import TextIO
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


def _oc_context_args() -> list[str]:
    """``--context`` args for every ``oc`` call, or empty to use the current context.

    Pinning the context (WXD_OPENSHIFT_CONTEXT) keeps the uploader from inheriting a
    stray default kubeconfig context and talking to the wrong cluster.
    """
    context = os.getenv("WXD_OPENSHIFT_CONTEXT")
    return ["--context", context] if context else []


def _oc_secret_value(secret_name: str, key: str, namespace: str) -> str | None:
    try:
        print(f"  Reading secret {namespace}/{secret_name} key '{key}' via oc...", file=sys.stderr)
        result = subprocess.run(
            [
                "oc",
                *_oc_context_args(),
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
            timeout=30,
        )
        if not result.stdout:
            return None
        decoded = subprocess.run(
            ["base64", "--decode"],
            input=result.stdout,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return decoded.stdout
    except Exception as exc:
        # Previously swallowed silently; surface it so the user can see WHY the
        # oc-based credential lookup failed (e.g. not logged in, RBAC, timeout).
        print(
            f"  [warn] could not read {namespace}/{secret_name} key '{key}': {exc}",
            file=sys.stderr,
        )
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


def _open_port_forward_log() -> tuple[Path, TextIO]:
    """Open a writable port-forward log, preferring runtime-writable locations."""
    explicit_log = os.getenv("WXD_OBJECT_STORE_PORT_FORWARD_LOG")
    if explicit_log:
        candidates = [Path(explicit_log)]
    else:
        candidate_dirs = [
            os.getenv("WXD_OBJECT_STORE_LOG_DIR"),
            Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "logs",
            ROOT / "logs",
            Path(tempfile.gettempdir()),
        ]
        candidates = [
            Path(candidate_dir) / "minio-port-forward.log"
            for candidate_dir in candidate_dirs
            if candidate_dir
        ]

    errors: list[str] = []
    for log_path in candidates:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            return log_path, log_path.open("w")
        except OSError as exc:
            errors.append(f"{log_path}: {exc}")

    raise SystemExit(
        "Unable to open a writable MinIO port-forward log. Tried:\n  "
        + "\n  ".join(errors)
    )


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
    log_path, log_file = _open_port_forward_log()

    process = subprocess.Popen(
        [
            "oc",
            *_oc_context_args(),
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
    log_file.close()

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
        import botocore.config
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
