#!/usr/bin/env python3
"""Generate and stage dbt artifacts for OpenMetadata ingestion."""

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
