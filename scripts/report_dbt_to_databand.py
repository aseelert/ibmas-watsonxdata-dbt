#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  report_dbt_to_databand.py — report a completed dbt run to Databand (optional)
#
#  Location  : scripts/report_dbt_to_databand.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-07-02) — Initial version. dbt-level Databand tracking, added
#      after confirming dbnd-airflow / dbnd-airflow-monitor / dbnd-airflow-
#      auto-tracking (all pinned to 1.0.34.1 as of this writing) are broken on
#      Airflow 3.x: their code hard-imports Airflow-1.x-only paths
#      (airflow.hooks.base_hook, airflow.operators.bash_operator,
#      airflow.operators.subdag_operator) that don't exist in Airflow 2.x or
#      3.x, and separately reference the DagRun.execution_date column removed
#      in Airflow 3's schema. This script instead uses dbnd's CORE package
#      (zero Airflow dependency) and its native dbt-artifact reader, so it
#      works regardless of Airflow version — see airflow/README.md "Optional:
#      Databand tracking" for the full investigation.
# -----------------------------------------------------------------------------
"""Report a completed dbt run's manifest/run_results to Databand.

WHAT it does: reads the dbt artifacts already sitting in ``target/`` after a
normal ``dbt run`` (``manifest.json``, ``run_results.json``, plus
``dbt_project.yml`` and the dbt profile) and reports them to a Databand tenant
using dbnd's core tracking SDK (``dbnd.providers.dbt.dbt_core.
collect_data_from_dbt_core``). This is deliberately NOT the Airflow-level
integration (dbnd-airflow / dbnd-airflow-monitor) — those packages are broken
on Airflow 3.x, see the changelog above. This script only needs the ``dbnd``
core package, which has no Airflow dependency at all.

WHEN to run it: immediately after a dbt run/test you want visible in
Databand — e.g. as a step after ``scripts/dbt_env.sh run`` and
``scripts/dbt_env.sh test``. It reports whatever is CURRENTLY in ``target/``;
it does not run dbt itself (unlike scripts/prepare_openmetadata_dbt_artifacts.py).

ENV VARS read (from .env via python-dotenv, or already-exported):
 - ``DBND__CORE__DATABAND_URL`` — Databand tenant URL. Required for anything
   beyond --dry-run; when unset, this script prints a message and exits 0
   (Databand tracking is an OPTIONAL feature, same as the rest of the demo).
 - ``DBND__CORE__DATABAND_ACCESS_TOKEN`` — Databand access token (SECRET).

PREREQUISITES: ``target/manifest.json`` and ``target/run_results.json`` must
already exist (run ``scripts/dbt_env.sh run`` first), and the ``dbt`` package
import path must resolve ``dbnd`` (add it to requirements.txt / the active
venv — see airflow/README.md for the install note).

USAGE examples:
 - ``python3 scripts/report_dbt_to_databand.py --dry-run``
     Validate everything (artifacts present, env vars set, project/invocation
     IDs parse correctly) WITHOUT starting a tracker, WITHOUT touching the
     network, and WITHOUT the log-rotation side effect below. Safe to run
     repeatedly.
 - ``python3 scripts/report_dbt_to_databand.py``
     Actually report the run to Databand. Requires DBND__CORE__DATABAND_URL /
     DBND__CORE__DATABAND_ACCESS_TOKEN to be set.

SIDE EFFECTS (non-dry-run only) + EXIT:
 - Reads ``logs/dbt.log``, copies it to a timestamped backup
   (``logs/dbt.log.<timestamp>``), and TRUNCATES the original — this is
   dbnd's own internal behavior (dbnd.providers.dbt.dbt_core), not something
   this wrapper adds. It only happens once a tracker is active, i.e. never
   under --dry-run.
 - Sends the parsed dbt metadata to the Databand tenant over HTTPS.
 - Exits 0 on success (including the no-op "not configured" case), non-zero
   on a real failure (missing artifacts, dbnd import error, etc).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_ARTIFACTS = ["manifest.json", "run_results.json"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Report a completed dbt run to Databand (optional feature)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Validate artifacts/config and parse project+invocation IDs only. "
            "Never starts a tracker, never touches the network, never rotates "
            "logs/dbt.log."
        ),
    )
    parser.add_argument(
        "--dbt-project-path",
        default=str(ROOT),
        help="Path containing target/, dbt_project.yml, logs/ (default: repo root).",
    )
    args = parser.parse_args()

    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    dbt_project_path = Path(args.dbt_project_path).expanduser().resolve()
    print("== report dbt run to Databand ==")
    print(f"dbt project path: {dbt_project_path}")
    print(f"dry-run: {args.dry_run}")

    missing = [
        name
        for name in REQUIRED_ARTIFACTS
        if not (dbt_project_path / "target" / name).exists()
    ]
    if missing:
        raise SystemExit(
            "Missing dbt artifacts in target/: "
            + ", ".join(missing)
            + ". Run `scripts/dbt_env.sh run` first."
        )

    databand_url = os.getenv("DBND__CORE__DATABAND_URL")
    databand_token = os.getenv("DBND__CORE__DATABAND_ACCESS_TOKEN")

    # Parsed directly from JSON (not dbnd's _load_dbt_core_assets(), which
    # unconditionally reads+rotates logs/dbt.log as a side effect — see the
    # module docstring). Safe to do in every mode, including --dry-run.
    manifest = json.loads((dbt_project_path / "target" / "manifest.json").read_text())
    run_results = json.loads((dbt_project_path / "target" / "run_results.json").read_text())
    project_name = manifest.get("metadata", {}).get("project_name")
    invocation_id = run_results.get("metadata", {}).get("invocation_id")
    print(f"dbt project name: {project_name}")
    print(f"dbt project id:   {manifest.get('metadata', {}).get('project_id')}")
    print(f"dbt invocation:   {invocation_id}")

    if args.dry_run:
        try:
            import dbnd  # noqa: F401

            dbnd_status = "OK"
        except ImportError as exc:
            dbnd_status = f"NOT INSTALLED ({exc})"
        print()
        print(f"dbnd importable: {dbnd_status}")
        print(f"DBND__CORE__DATABAND_URL set: {bool(databand_url)}")
        print(f"DBND__CORE__DATABAND_ACCESS_TOKEN set: {bool(databand_token)}")
        print()
        print("[dry-run] validation OK. Not starting a tracker, not sending, not touching logs/dbt.log.")
        return 0

    try:
        import dbnd  # noqa: F401
    except ImportError as exc:
        raise SystemExit(
            f"dbnd is not installed in this environment ({exc}). "
            "Add `dbnd` to requirements.txt / the active venv."
        )

    if not databand_url or not databand_token:
        print()
        print(
            "DBND__CORE__DATABAND_URL / DBND__CORE__DATABAND_ACCESS_TOKEN not set — "
            "Databand tracking is optional and disabled. Nothing to do."
        )
        return 0

    from dbnd import dbnd_tracking_start, dbnd_tracking_stop
    from dbnd.providers.dbt.dbt_core import collect_data_from_dbt_core

    print()
    print(f"reporting dbt run {invocation_id} to {databand_url} ...")
    dbnd_tracking_start(job_name=project_name, run_name=invocation_id)
    try:
        collect_data_from_dbt_core(str(dbt_project_path))
    finally:
        dbnd_tracking_stop()
    print("done.")
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
