#!/usr/bin/env bash
# -----------------------------------------------------------------------------
#  dbt_env.sh — dbt launcher that loads the demo .env and the repo virtualenv.
#
#  Location  : scripts/dbt_env.sh
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.1 (2026-07-02) — Added optional post-run Databand reporting (see SIDE
#      EFFECTS below). No longer a pure `exec` passthrough for commands that
#      produce dbt run artifacts.
#
#  WHAT / WHY
#    A thin wrapper around the `dbt` CLI. It guarantees that every dbt
#    invocation in the demo runs with the SAME environment that the rest of
#    the tooling expects: the demo's `.env` is exported (so connection settings
#    such as WXD_HOST / WXD_USER / WXD_PASSWORD / WXD_SSL_VERIFY and friends are
#    visible to the dbt-presto adapter and to profiles.yml's env_var() lookups),
#    and the project virtualenv's pinned dbt binary is preferred over whatever
#    `dbt` happens to be on PATH. This removes "works on my machine" drift.
#
#  WHEN TO RUN IT
#    Anywhere you would normally type `dbt`. Use it for `dbt debug`, `dbt run`,
#    `dbt test`, `dbt build`, etc., across the bronze/silver/gold medallion
#    layers. Nothing must run before it beyond having a populated `.env` and
#    (ideally) the `.venv` created — but it degrades gracefully if either is
#    missing (see below).
#
#  ENV VARS
#    Reads NONE directly; instead it SOURCES `<repo>/.env` (with `set -a`, so
#    every assignment in that file is exported to the dbt child process). The
#    actual variable names live in `.env` (e.g. WXD_HOST, WXD_PORT, WXD_USER,
#    WXD_PASSWORD, WXD_CATALOG, WXD_SCHEMA, WXD_SSL_VERIFY, and optionally
#    DBND__CORE__DATABAND_URL / DBND__CORE__DATABAND_ACCESS_TOKEN — see below).
#
#  PREREQUISITES
#    None hard. If `<repo>/.env` is absent it is silently skipped (dbt then
#    relies on the ambient environment). If `<repo>/.venv/bin/dbt` is absent it
#    falls back to the first `dbt` on PATH.
#
#  USAGE
#    scripts/dbt_env.sh debug
#    scripts/dbt_env.sh run  --select bronze
#    scripts/dbt_env.sh test --select silver+
#
#  SIDE EFFECTS / EXIT
#    dbt's own exit code is always returned verbatim to the caller (0 on
#    success, non-zero on dbt errors) — this wrapper never masks it, even when
#    the Databand reporting step below fails.
#
#    For `seed`, `run`, `test`, `build`, and `snapshot` ONLY: if
#    DBND__CORE__DATABAND_URL is set (via .env), this script automatically
#    calls `scripts/report_dbt_to_databand.py` after dbt finishes, reporting
#    the run to Databand. This is entirely optional — unset
#    DBND__CORE__DATABAND_URL and nothing changes from a pure dbt passthrough.
#    When it does run, be aware that report_dbt_to_databand.py rotates
#    `logs/dbt.log` (backs it up, then truncates it) as dbnd's own internal
#    behavior — see that script's docstring. Reporting failures are logged as
#    warnings and never change this script's exit code.
# -----------------------------------------------------------------------------
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${repo_root}/.env" ]]; then
  echo "[dbt_env] loading environment from ${repo_root}/.env" >&2
  set -a
  # shellcheck disable=SC1091
  source "${repo_root}/.env"
  set +a
else
  echo "[dbt_env] no .env at ${repo_root}/.env — using the ambient environment" >&2
fi

if [[ -x "${repo_root}/.venv/bin/dbt" ]]; then
  dbt_bin="${repo_root}/.venv/bin/dbt"
  echo "[dbt_env] using virtualenv dbt: ${dbt_bin} $*" >&2
else
  dbt_bin="dbt"
  echo "[dbt_env] virtualenv dbt not found — falling back to dbt on PATH: dbt $*" >&2
fi

if "${dbt_bin}" "$@"; then
  dbt_exit=0
else
  dbt_exit=$?
fi

case "${1:-}" in
  seed|run|test|build|snapshot)
    if [[ -n "${DBND__CORE__DATABAND_URL:-}" ]]; then
      echo "[dbt_env] DBND__CORE__DATABAND_URL is set — reporting this run to Databand" >&2
      python_bin="${repo_root}/.venv/bin/python3"
      [[ -x "${python_bin}" ]] || python_bin="python3"
      if ! "${python_bin}" "${repo_root}/scripts/report_dbt_to_databand.py"; then
        echo "[dbt_env] WARNING: Databand reporting failed (non-fatal; dbt's own exit code is preserved)" >&2
      fi
    fi
    ;;
esac

exit "${dbt_exit}"
