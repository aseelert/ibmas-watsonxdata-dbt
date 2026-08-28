#!/usr/bin/env python3
"""
emit_openlineage_events.py — Post-run OpenLineage event emitter for dbt.

Why this script exists
----------------------
The `dbt-ol` wrapper (openlineage-dbt) does not recognise the `watsonx_presto`
adapter type.  Its internal Adapter enum only covers adapters explicitly listed
in openlineage-common's DbtLocalArtifactProcessor.  Watsonx Presto is
Presto/Trino-family, so we:

  1. Extend the Adapter enum at runtime to add WATSONX_PRESTO.
  2. Patch DbtArtifactProcessor.extract_adapter_type to map WATSONX_PRESTO → TRINO
     (used for adapter type selection in the processor).
  3. Patch DbtArtifactProcessor.extract_namespace to return a trino:// URI
     (used to build the dataset namespace that appears in Marquez).

Usage (called automatically by scripts/02_dbt_env.sh after a successful dbt run)
-----------------
    python3 scripts/emit_openlineage_events.py [--target-path ./target] [--project-dir ./]

Environment
-----------
    OPENLINEAGE_URL       HTTP URL of the Marquez API.
                          Priority:
                            1. OPENLINEAGE_URL (explicit override)
                            2. http://localhost:5010   (local docker-compose default)
                          OCP Route example:
                            https://marquez-api.apps.watson.ibmas-zocp-techcluster.org
                          In-cluster (Spark pods in cpd-instance):
                            http://marquez.cpd-instance.svc.cluster.local:5000
    OPENLINEAGE_NAMESPACE Namespace shown in Marquez UI (default: dbt_demo)
    DBT_PROFILES_DIR      Path to profiles/            (set by 02_dbt_env.sh)
"""

import logging
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Load .env BEFORE reading any environment variables.
# emit_openlineage_events.py is called as a subprocess by 02_dbt_env.sh,
# which does NOT export its own `export` statements into the child process.
# Using python-dotenv here ensures OPENLINEAGE_URL, OPENLINEAGE_NAMESPACE, and
# other vars from .env are available even when the script is invoked directly.
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv as _load_dotenv
    _env_file = Path(__file__).resolve().parent.parent / ".env"
    if _env_file.exists():
        _load_dotenv(_env_file, override=False)   # override=False: real env vars win
except ImportError:
    pass   # python-dotenv not installed; rely on the shell environment

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("openlineage.emit")

# ---------------------------------------------------------------------------
# Resolve paths from CLI args (mirrors what dbt-ol accepts)
# ---------------------------------------------------------------------------
def _parse_arg(args: list[str], flags: list[str], default: str | None = None) -> str | None:
    for flag in flags:
        try:
            idx = args.index(flag)
            return args[idx + 1]
        except (ValueError, IndexError):
            pass
    return default


args = sys.argv[1:]
project_dir  = _parse_arg(args, ["--project-dir"], default="./")
target_path  = _parse_arg(args, ["--target-path"])
target       = _parse_arg(args, ["-t", "--target"])
profile_name = _parse_arg(args, ["--profile"])

# Auto-detect profile_name from dbt_project.yml when not supplied via --profile.
# The key is `profile: <name>` at the top level.
if not profile_name:
    import re as _re
    _project_yml = os.path.join(project_dir or "./", "dbt_project.yml")
    try:
        with open(_project_yml) as _f:
            for _line in _f:
                _m = _re.match(r"^profile:\s*['\"]?([^'\"#\s]+)", _line)
                if _m:
                    profile_name = _m.group(1)
                    break
    except OSError:
        pass
    if not profile_name:
        profile_name = "watsonxdata_medallion_demo"   # canonical fallback for this project

# ---------------------------------------------------------------------------
# Monkey-patch: teach openlineage-common that watsonx_presto == trino
# ---------------------------------------------------------------------------
try:
    from openlineage.common.provider.dbt.processor import Adapter, DbtArtifactProcessor
    from openlineage.common.provider.dbt.local import DbtLocalArtifactProcessor  # noqa: F401

    # ── 1. Extend the Adapter enum ──────────────────────────────────────────
    # Python enums are mutable via their internal maps; adding a new member
    # here means Adapter["WATSONX_PRESTO"] and Adapter("watsonx_presto") work.
    if "WATSONX_PRESTO" not in Adapter._member_names_:
        new_member         = object.__new__(Adapter)
        new_member._name_  = "WATSONX_PRESTO"
        new_member._value_ = "watsonx_presto"
        Adapter._member_map_["WATSONX_PRESTO"]       = new_member
        Adapter._value2member_map_["watsonx_presto"] = new_member
        Adapter._member_names_.append("WATSONX_PRESTO")
        logger.info("Patched Adapter enum: WATSONX_PRESTO added")

    # ── 2. Patch extract_adapter_type ───────────────────────────────────────
    # The original method does Adapter[profile["type"].upper()] which now
    # succeeds for WATSONX_PRESTO, but we keep the TRINO fallback in case
    # the enum extension races or a subclass overrides things.
    _orig_extract_type = DbtArtifactProcessor.extract_adapter_type

    def _patched_extract_type(self, profile: dict) -> None:
        try:
            _orig_extract_type(self, profile)
        except NotImplementedError:
            logger.info(
                "Adapter type '%s' not natively supported; treating as TRINO.",
                profile.get("type"),
            )
            self.adapter_type = Adapter.TRINO

    DbtArtifactProcessor.extract_adapter_type = _patched_extract_type

    # ── 3. Patch extract_namespace ──────────────────────────────────────────
    # extract_namespace has a long elif chain that does NOT cover the new
    # WATSONX_PRESTO member and falls through to raise NotImplementedError.
    # We intercept before the original call for WATSONX_PRESTO, delegate
    # everything else to the original implementation.
    _orig_extract_ns = DbtArtifactProcessor.extract_namespace

    def _patched_extract_ns(self, profile: dict) -> str:
        if (
            self.adapter_type is not None
            and getattr(self.adapter_type, "_value_", None) == "watsonx_presto"
        ):
            host = profile.get("host", "localhost")
            port = profile.get("port", 443)
            logger.info("Resolved namespace: trino://%s:%s", host, port)
            return f"trino://{host}:{port}"
        return _orig_extract_ns(self, profile)

    DbtArtifactProcessor.extract_namespace = _patched_extract_ns
    logger.info("Patched DbtArtifactProcessor: extract_adapter_type + extract_namespace")

except ImportError as exc:
    logger.error(
        "openlineage-dbt not installed: %s\n"
        "  Fix: pip install -r requirements.txt   (or: pip install 'openlineage-dbt>=1.18,<2.0')",
        exc,
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Resolve the Marquez endpoint (OPENLINEAGE_URL, falling back to localhost)
# ---------------------------------------------------------------------------
_DEFAULT_URL = "http://localhost:5010"
_ol_url = os.environ.get("OPENLINEAGE_URL", "").strip()
if not _ol_url:
    _ol_url = _DEFAULT_URL
    logger.info(
        "OPENLINEAGE_URL not set — using local default: %s  "
        "(set OPENLINEAGE_URL=https://marquez-api.<app-domain> to target the OCP route)",
        _ol_url,
    )
else:
    logger.info("Emitting OpenLineage events to: %s", _ol_url)

# The openlineage-dbt library reads OPENLINEAGE_URL from the environment; write
# it back so the correct URL is used regardless of how it was resolved above.
os.environ["OPENLINEAGE_URL"] = _ol_url
os.environ.setdefault("OPENLINEAGE_NAMESPACE", "dbt_demo")

from openlineage.dbt import consume_local_artifacts  # noqa: E402

rc = consume_local_artifacts(
    args=["send-events"],            # skip re-running dbt; just read target/ artifacts
    target=target or "dev",          # str required; fall back to dbt default profile target
    target_path=target_path,         # str | None — accepted by library
    project_dir=project_dir or "./", # str required; should always be set
    profile_name=profile_name,       # resolved above (auto-detected from dbt_project.yml)
    model_selector="",               # str required; empty = all models
    models=[],
    openlineage_job_name=None,
)
sys.exit(rc or 0)
