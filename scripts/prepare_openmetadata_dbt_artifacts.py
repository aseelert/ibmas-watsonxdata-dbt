#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  prepare_openmetadata_dbt_artifacts.py — build dbt artifacts and stage them locally for OpenMetadata
#
#  Location  : scripts/prepare_openmetadata_dbt_artifacts.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Generate and stage dbt artifacts for OpenMetadata ingestion.

This script is the FIRST half of the OpenMetadata lineage workflow: it makes
sure fresh dbt artifacts (``manifest.json``, ``catalog.json``,
``run_results.json``) exist in the project ``target/`` directory and then
copies them into a stable staging directory that OpenMetadata can read. The
companion script ``scripts/upload_dbt_artifacts.py`` later pushes those staged
files to S3-compatible object storage. OpenMetadata consumes the manifest to
reconstruct dbt model/table lineage and column-level metadata for the
watsonx.data medallion demo.

WHAT it does:
 - Optionally runs the full dbt pipeline against watsonx.data via the
   ``scripts/dbt_env.sh`` wrapper: ``seed --full-refresh`` (unless
   ``--skip-seed``), then ``run``, ``test`` and ``docs generate``. The
   ``docs generate`` step is what emits ``catalog.json`` with rich column
   metadata.
 - Each dbt command runs with a hard ``DBT_TIMEOUT`` (600s) cap and a
   configurable retry count so a hung or transiently failing dbt invocation
   cannot block the demo forever.
 - Copies the required + optional artifacts out of ``target/`` into the
   staging directory, announcing each staged file.
 - Validates that the REQUIRED artifact (``manifest.json``) is present and
   exits non-zero with a clear message if it is missing.

WHEN to run it: after the dbt project is configured and able to reach
watsonx.data (i.e. ``.env`` is populated and the Presto/Iceberg engine is
running). Run this BEFORE ``scripts/upload_dbt_artifacts.py``, which depends on
the files this script stages. Use ``--skip-dbt`` to re-stage already-built
artifacts without re-hitting the live engine.

ENV VARS read:
 - ``WXD_DBT_ARTIFACT_DIR`` — staging directory for the artifacts
   (default: ``openmetadata/dbt-artifacts``, resolved relative to repo root
   when not absolute). Overridable per-run via ``--artifact-dir``.
 - Plus whatever ``scripts/dbt_env.sh`` itself reads to authenticate against
   watsonx.data (loaded here from ``.env`` via python-dotenv when available).

PREREQUISITES: a working dbt profile pointing at watsonx.data, the
``scripts/dbt_env.sh`` wrapper, and (when not using ``--skip-dbt``) a reachable
Presto engine. No ``oc login`` / ``cpdctl`` is required by this script itself.

USAGE examples:
 - ``python3 scripts/prepare_openmetadata_dbt_artifacts.py``
     full pipeline (seed + run + test + docs) then stage.
 - ``python3 scripts/prepare_openmetadata_dbt_artifacts.py --skip-seed``
     skip the seed refresh but still run/test/docs.
 - ``python3 scripts/prepare_openmetadata_dbt_artifacts.py --skip-dbt``
     only re-copy existing ``target/*.json`` into the staging directory.
 - ``python3 scripts/prepare_openmetadata_dbt_artifacts.py \\
       --artifact-dir /tmp/om-dbt --retries 3``
     custom staging dir and 3 retries per dbt command.

SIDE EFFECTS + EXIT: creates/overwrites files in the staging directory,
prints the staged file paths, and returns exit code 0 on success. Raises
``SystemExit`` (non-zero) when ``manifest.json`` is missing, and re-raises the
underlying error if a dbt command exhausts its retries.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_ARTIFACTS = ["manifest.json"]
OPTIONAL_ARTIFACTS = ["catalog.json", "run_results.json"]
ARTIFACTS = REQUIRED_ARTIFACTS + OPTIONAL_ARTIFACTS

# Hard cap on each dbt subprocess so a hung dbt can't block forever (which would
# otherwise defeat the retry cap). 10 min is well within the demo's time budget.
DBT_TIMEOUT = 600


def _run(command: list[str], retries: int) -> None:
    for attempt in range(retries + 1):
        print(f"$ {' '.join(command)}  (attempt {attempt + 1}/{retries + 1}, timeout {DBT_TIMEOUT}s)")
        try:
            subprocess.run(command, cwd=ROOT, check=True, timeout=DBT_TIMEOUT)
            print(f"[OK] {' '.join(command)}")
            return
        except subprocess.TimeoutExpired:
            print(f"[FAIL] command exceeded {DBT_TIMEOUT}s and was killed.")
            if attempt >= retries:
                raise
            wait_seconds = 5 * (attempt + 1)
            print(f"Command timed out; retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)
        except subprocess.CalledProcessError:
            print("[FAIL] command exited non-zero.")
            if attempt >= retries:
                raise
            wait_seconds = 5 * (attempt + 1)
            print(f"Command failed; retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)


def _env_path(name: str, default: str) -> Path:
    value = os.getenv(name, default)
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate dbt artifacts and stage them for OpenMetadata."
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Only copy existing target/*.json artifacts; do not run dbt commands.",
    )
    parser.add_argument(
        "--skip-seed",
        action="store_true",
        help="Do not refresh dbt seed tables before running dbt models.",
    )
    parser.add_argument(
        "--artifact-dir",
        help="Directory where OpenMetadata-readable artifacts should be staged.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=1,
        help="Number of retries for each dbt command. Default: 1.",
    )
    args = parser.parse_args()

    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    print("== prepare OpenMetadata dbt artifacts ==")
    print(f"repo root: {ROOT}")
    print(f"skip-dbt={args.skip_dbt} skip-seed={args.skip_seed} retries={args.retries}")

    if not args.skip_dbt:
        if not args.skip_seed:
            _run(["scripts/dbt_env.sh", "seed", "--full-refresh"], args.retries)
        _run(["scripts/dbt_env.sh", "run"], args.retries)
        _run(["scripts/dbt_env.sh", "test"], args.retries)
        _run(["scripts/dbt_env.sh", "docs", "generate"], args.retries)

    source_dir = ROOT / "target"
    target_dir = (
        Path(args.artifact_dir).expanduser()
        if args.artifact_dir
        else _env_path("WXD_DBT_ARTIFACT_DIR", "openmetadata/dbt-artifacts")
    )
    if not target_dir.is_absolute():
        target_dir = ROOT / target_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    print(f"staging artifacts from {source_dir} -> {target_dir}")

    missing_required = [
        name for name in REQUIRED_ARTIFACTS if not (source_dir / name).exists()
    ]
    missing_optional = [
        name for name in OPTIONAL_ARTIFACTS if not (source_dir / name).exists()
    ]
    if missing_required:
        raise SystemExit(
            "Missing required dbt artifacts in target/: "
            + ", ".join(missing_required)
            + ". Run without --skip-dbt first."
        )

    for name in ARTIFACTS:
        source = source_dir / name
        if source.exists():
            shutil.copy2(source, target_dir / name)
            print(f"staged {target_dir / name}")

    if missing_optional:
        print()
        print(
            "Optional artifacts missing: "
            + ", ".join(missing_optional)
            + ". OpenMetadata can use manifest.json, but catalog.json adds richer column metadata."
        )

    print()
    print("OpenMetadata local file paths:")
    for name in ARTIFACTS:
        path = target_dir / name
        if path.exists():
            print(f"  {name}: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
