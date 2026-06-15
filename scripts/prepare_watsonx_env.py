#!/usr/bin/env python3
"""Prepare local watsonx.data demo configuration from a connection JSON export.

The script reads a watsonx.data Presto connection JSON, writes the embedded
certificate chain to certs/watsonxdata-ca.pem, and updates .env with the
non-secret connection values. Existing secrets such as WXD_API_KEY are kept.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONNECTION_JSON = ROOT / "watsonx_data" / "instance_details.json"
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_CERT_FILE = ROOT / "certs" / "watsonxdata-ca.pem"


def _load_connection(path: Path) -> dict[str, str]:
    try:
        payload = json.loads(path.read_text())
    except FileNotFoundError as exc:
        raise SystemExit(
            f"Connection JSON not found: {path}\n"
            "Export the watsonx.data Presto connection JSON and save it there, "
            "or pass --connection-json /path/to/file.json."
        ) from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc

    connection = payload.get("properties", {}).get("connection", [])
    values = {
        str(item.get("name")): str(item.get("value"))
        for item in connection
        if item.get("name") and item.get("value") is not None
    }
    if not values:
        raise SystemExit(
            f"{path} does not look like a watsonx.data connection export. "
            "Expected properties.connection entries with name/value pairs."
        )
    return values


def _read_env(path: Path) -> tuple[list[tuple[str, str | None]], dict[str, str]]:
    if not path.exists():
        return [], {}

    items: list[tuple[str, str | None]] = []
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            items.append((line, None))
            continue
        key, value = line.split("=", 1)
        items.append((key, value))
        values[key] = value
    return items, values


def _write_env(path: Path, items: list[tuple[str, str | None]], values: dict[str, str]) -> None:
    seen: set[str] = set()
    lines: list[str] = []
    for key, old_value in items:
        if old_value is None:
            lines.append(key)
            continue
        if key in seen:
            continue
        lines.append(f"{key}={values[key]}")
        seen.add(key)

    new_keys = [key for key in values if key not in seen]
    if new_keys:
        if lines and lines[-1] != "":
            lines.append("")
        lines.append("# Imported from watsonx_data/instance_details.json")
        for key in new_keys:
            lines.append(f"{key}={values[key]}")

    path.write_text("\n".join(lines).rstrip() + "\n")


def _cert_chain(value: str) -> str:
    certificates = re.findall(
        r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
        value,
        flags=re.DOTALL,
    )
    if not certificates:
        raise SystemExit("ssl_certificate did not contain a PEM certificate chain.")
    return "\n".join(cert.strip() for cert in certificates) + "\n"


def _set(values: dict[str, str], key: str, value: str | None, overwrite: bool) -> None:
    if not value:
        return
    if overwrite or key not in values or values[key] == "":
        values[key] = value


def _relative_to_root(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import watsonx.data Presto connection details into .env."
    )
    parser.add_argument(
        "--connection-json",
        default=str(DEFAULT_CONNECTION_JSON),
        help="Path to the watsonx.data Presto connection JSON export.",
    )
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_FILE),
        help="Path to the .env file to create or update.",
    )
    parser.add_argument(
        "--cert-file",
        default=str(DEFAULT_CERT_FILE),
        help="Path where the ssl_certificate PEM chain should be written.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing non-secret .env values with values from the JSON.",
    )
    args = parser.parse_args()

    connection_path = Path(args.connection_json).expanduser()
    env_path = Path(args.env_file).expanduser()
    cert_path = Path(args.cert_file).expanduser()
    if not connection_path.is_absolute():
        connection_path = ROOT / connection_path
    if not env_path.is_absolute():
        env_path = ROOT / env_path
    if not cert_path.is_absolute():
        cert_path = ROOT / cert_path

    connection = _load_connection(connection_path)
    items, env_values = _read_env(env_path)

    _set(env_values, "WXD_INSTANCE_ID", connection.get("instance_id"), args.overwrite)
    _set(env_values, "WXD_HOST", connection.get("engine_host"), args.overwrite)
    _set(
        env_values,
        "WXD_PORT",
        connection.get("engine_port") or connection.get("port"),
        args.overwrite,
    )
    _set(env_values, "WXD_PRESTO_ENGINE_ID", connection.get("engine_id"), args.overwrite)

    cpd_host = connection.get("host")
    _set(env_values, "WXD_CPD_HOST", cpd_host, args.overwrite)
    if cpd_host:
        _set(
            env_values,
            "WXD_CPD_AUTH_URL",
            f"https://{cpd_host}/icp4d-api/v1/authorize",
            args.overwrite,
        )

    ssl_certificate = connection.get("ssl_certificate")
    if ssl_certificate:
        cert_path.parent.mkdir(parents=True, exist_ok=True)
        cert_path.write_text(_cert_chain(ssl_certificate))
        _set(env_values, "WXD_SSL_VERIFY", _relative_to_root(cert_path), True)

    if "WXD_CATALOG" not in env_values:
        env_values["WXD_CATALOG"] = "iceberg_data"
    if "WXD_SCHEMA" not in env_values:
        env_values["WXD_SCHEMA"] = "lakehouse_demo"
    if "WXD_GOLD_MATERIALIZED" not in env_values:
        env_values["WXD_GOLD_MATERIALIZED"] = "view"

    cpd_username = env_values.get("WXD_CPD_USERNAME")
    if not cpd_username:
        wxd_user = env_values.get("WXD_USER", "")
        if wxd_user.startswith("ibmlhapikey_"):
            cpd_username = wxd_user.removeprefix("ibmlhapikey_")
            env_values["WXD_CPD_USERNAME"] = cpd_username

    if cpd_username and "WXD_USER" not in env_values:
        env_values["WXD_USER"] = f"ibmlhapikey_{cpd_username}"

    _write_env(env_path, items, env_values)

    print(f"Read connection details from: {_relative_to_root(connection_path)}")
    if ssl_certificate:
        print(f"Wrote certificate chain to: {_relative_to_root(cert_path)}")
    print(f"Updated env file: {_relative_to_root(env_path)}")
    print()
    print("Imported values:")
    for key in [
        "WXD_INSTANCE_ID",
        "WXD_HOST",
        "WXD_PORT",
        "WXD_PRESTO_ENGINE_ID",
        "WXD_CPD_HOST",
        "WXD_CPD_AUTH_URL",
        "WXD_SSL_VERIFY",
        "WXD_CATALOG",
        "WXD_SCHEMA",
    ]:
        if key in env_values:
            print(f"  {key}={env_values[key]}")
    if "WXD_API_KEY" not in env_values:
        print()
        print("Next step: add WXD_API_KEY to .env. It is not stored in the connection JSON.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
