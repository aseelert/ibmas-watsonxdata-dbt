# =============================================================================
#  log.sh — sourceable bash helper library (logging, env, safety guards)
#
#  Location  : scripts/lib/log.sh
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Shared log helpers (info/warn/
#      success/error/step), load_env(), confirm(), run(), install_err_trap().
# =============================================================================
#
#  HOW TO USE (from any script in this repo):
#
#    #!/usr/bin/env bash
#    set -euo pipefail
#    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#    source "${SCRIPT_DIR}/../scripts/lib/log.sh"   # adjust path to reach here
#    install_err_trap        # nice "failed on line N" messages
#    load_env                # pull in <repo>/.env if it exists
#    step "Doing the thing"
#    run rm -rf /some/dir    # echoed in --dry-run, executed otherwise
#
#  WHY a shared library? Every script in this repo prints the SAME style of
#  log lines and reads the SAME .env file. Keeping that logic in one place
#  means a fix here improves every script at once (audience: an 18-year-old
#  learner — think of this as the "toolbox" all the other scripts borrow from).
# =============================================================================

# Guard against double-sourcing. If two scripts both source this file (or one
# sources another that already did), we only want to define everything once.
if [[ -n "${__LOG_SH_LOADED:-}" ]]; then
  return 0 2>/dev/null || true
fi
__LOG_SH_LOADED=1

# ---------------------------------------------------------------------------
# Colours — only used when stdout is an interactive terminal. When the output
# is piped to a file or another program, we emit plain text (no escape codes)
# so logs stay readable. NO_COLOR=1 also forces plain text (a common convention).
# ---------------------------------------------------------------------------
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  __C_RESET=$'\033[0m'
  __C_BLUE=$'\033[34m'
  __C_YELLOW=$'\033[33m'
  __C_GREEN=$'\033[32m'
  __C_RED=$'\033[31m'
  __C_BOLD=$'\033[1m'
else
  __C_RESET=''; __C_BLUE=''; __C_YELLOW=''; __C_GREEN=''; __C_RED=''; __C_BOLD=''
fi

# ---------------------------------------------------------------------------
# info <message>
#   Prints a normal, low-priority status line. Use for "here is what I am
#   doing right now" messages. Goes to stdout.
# ---------------------------------------------------------------------------
info()    { echo "${__C_BLUE}  [INFO]  ${__C_RESET}$*"; }

# ---------------------------------------------------------------------------
# success <message>
#   Prints a green "this worked" line. Use right after a step completes OK.
# ---------------------------------------------------------------------------
success() { echo "${__C_GREEN}  [OK]    ${__C_RESET}$*"; }

# ---------------------------------------------------------------------------
# warn <message>
#   Prints a yellow caution line for non-fatal problems (something looked odd
#   but we are carrying on). Goes to stderr so it stands out and does not
#   pollute captured stdout.
# ---------------------------------------------------------------------------
warn()    { echo "${__C_YELLOW}  [WARN]  ${__C_RESET}$*" >&2; }

# ---------------------------------------------------------------------------
# error <message>
#   Prints a red error line to stderr. This does NOT exit — the caller decides
#   whether to 'exit 1'. (Pair it with 'set -e' or an explicit exit.)
# ---------------------------------------------------------------------------
error()   { echo "${__C_RED}  [ERROR] ${__C_RESET}$*" >&2; }

# ---------------------------------------------------------------------------
# step <message>
#   Prints a bold section header with a leading blank line. Use it to mark the
#   start of a major phase ("▶  Step 3/5 — Start services").
# ---------------------------------------------------------------------------
step()    { echo ""; echo "${__C_BOLD}▶  $*${__C_RESET}"; }

# ---------------------------------------------------------------------------
# load_env [path]
#   Sources the repo's .env file so every VAR=value line becomes an exported
#   environment variable visible to this script AND any child process it spawns
#   (that is what 'set -a' does — auto-export everything that gets assigned).
#
#   - path defaults to <repo-root>/.env, where <repo-root> is two directories
#     above this file (scripts/lib/log.sh → repo root).
#   - If the file does not exist we just warn and continue: scripts should work
#     with real environment variables too, not only with a .env file.
#   - Returns 0 always (missing .env is not fatal).
# ---------------------------------------------------------------------------
load_env() {
  local lib_dir repo_root env_file
  lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  repo_root="$(cd "${lib_dir}/../.." && pwd)"     # scripts/lib → scripts → repo
  env_file="${1:-${repo_root}/.env}"

  if [[ -f "$env_file" ]]; then
    info "Loading environment from ${env_file}"
    set -a            # auto-export every variable assigned from here on
    # shellcheck disable=SC1090  # path is dynamic on purpose
    source "$env_file"
    set +a            # stop auto-exporting
  else
    warn "No .env found at ${env_file} — relying on the current shell environment."
  fi
  return 0
}

# ---------------------------------------------------------------------------
# confirm <question>
#   Asks the user a yes/no question before doing something destructive
#   (dropping a schema, deleting files, etc.). Returns 0 for "yes", 1 for "no".
#
#   Two global switches let scripts run unattended:
#     ASSUME_YES=true (or -y/--yes flag setting it)  → auto-answer "yes"
#     DRY_RUN=true                                   → auto-answer "yes" but the
#         run() helper below will only PRINT commands, so nothing is destroyed.
#
#   Always re-prompts on invalid input. Reads from /dev/tty so it still works
#   when the script's stdin is a pipe.
# ---------------------------------------------------------------------------
confirm() {
  local question="${1:-Are you sure?}" reply

  # Non-interactive overrides — skip the prompt entirely.
  if [[ "${ASSUME_YES:-false}" == "true" || "${DRY_RUN:-false}" == "true" ]]; then
    info "${question}  [auto-yes]"
    return 0
  fi

  while true; do
    # Prompt on stderr, read from the real terminal so pipes don't break it.
    printf "%s  [y/N] " "$question" >&2
    if ! read -r reply < /dev/tty 2>/dev/null; then
      reply=""   # no terminal available → treat as "no" (safe default)
    fi
    case "$reply" in
      [yY]|[yY][eE][sS]) return 0 ;;
      ""|[nN]|[nN][oO])  return 1 ;;
      *) warn "Please answer 'y' or 'n'." ;;
    esac
  done
}

# ---------------------------------------------------------------------------
# run <command> [args...]
#   Runs a command — UNLESS DRY_RUN=true, in which case it only prints what it
#   WOULD have run (prefixed with "+") and returns success. This lets any
#   script support a global --dry-run preview for free: just route every
#   side-effecting command through run().
#
#   Example:
#     run docker compose down        # executes normally
#     DRY_RUN=true run rm -rf /data  # prints "+ rm -rf /data", deletes nothing
# ---------------------------------------------------------------------------
run() {
  if [[ "${DRY_RUN:-false}" == "true" ]]; then
    echo "${__C_YELLOW}  + $*${__C_RESET} ${__C_BOLD}(dry-run — not executed)${__C_RESET}" >&2
    return 0
  fi
  "$@"
}

# ---------------------------------------------------------------------------
# install_err_trap
#   Installs a bash ERR trap so that when ANY command fails under 'set -e',
#   the script prints exactly which command failed and on which line before
#   exiting. Without this, a failing script just dies silently — very hard to
#   debug. Call this once near the top of every script (after 'set -euo
#   pipefail').
# ---------------------------------------------------------------------------
install_err_trap() {
  # ${BASH_SOURCE[1]}/${BASH_LINENO[0]} point at the failing line in the caller.
  # ${BASH_COMMAND} is the command that was about to run when the error fired.
  trap 'rc=$?; error "Command failed (exit ${rc}) at ${BASH_SOURCE[1]:-?}:${BASH_LINENO[0]:-?} → ${BASH_COMMAND}"' ERR
}
