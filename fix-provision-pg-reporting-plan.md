# Plan: Fix `10b_provision_pg_reporting.sh` — Complete Debug & Hardening

## Top-Level Overview

The script `scripts/10b_provision_pg_reporting.sh` has several correctness bugs (one **critical** 
blocker), missing error output, silent failure paths, and a password-desync logic error.  
The goal is to make every step emit clear INFO/DEBUG/ERROR output explaining what is happening 
and why things fail, and to fix all known bugs so the script runs end-to-end reliably.

No new features are added — only fixes and improved observability.

---

## Sub-Tasks

### Sub-Task 1 — Fix critical Crunchy v5 pod label selector

**Status:** `[x] done` — **but the premise below was WRONG. Reverted in v4.2. Do not re-apply.**

> **Correction (2026-08-19).** The claim "`role=master` was deprecated, the correct label is
> `role=primary`" is **backwards**, and applying it is what caused the reported 3-minute timeout:
> the selector matched nothing, every run died at Step 0.
>
> Verified against the Crunchy PGO v5 source (`internal/naming/labels.go`):
> * `RolePatroniLeader = "master"` — applied to the **leader Pod**. This is Patroni's own role
>   value and is what a pod selector must match.
> * `RolePrimary = "primary"` — only ever applied to the **Service** (`<cluster>-primary`),
>   never to a pod.
>
> v4.2 therefore tries `master`, `primary`, `leader` in that order and, if no role label
> matches, falls back to probing `pg_is_in_recovery()` on each `data=postgres` pod (`f` = primary).
> That fallback is label-independent, so a future upstream relabelling cannot break it again.
>
> The same mistake produced a second regression: `PG_SVC="${CLUSTER_NAME}-rw"`. `-rw`/`-ro`/`-r`
> is **CloudNativePG/EDB** naming (which CPD's own internal Postgres uses, hence the confusion);
> Crunchy's read-write Service is `<cluster>-primary`. v4.2 resolves the Service against the
> live cluster instead of assuming a suffix.

**Original (incorrect) intent:** The pod label `role=master` was deprecated in Crunchy Postgres
for Kubernetes v5. The correct label is `role=primary`. This is a hard blocker — the script always
fails to find the primary pod and dies immediately.

**Expected Outcomes:**
- Pod selector correctly finds the Running primary pod on Crunchy v5 clusters.
- A fallback selector also tries `role=primary` if `role=master` returns empty, with a debug 
  message explaining which label matched.

**Todo List:**
1. Change label selector on line 239 (inside wait loop) from `role=master` → `role=primary`
2. Change label selector on line 254 (pod locate block) from `role=master` → `role=primary`
3. Add `debug` log when pod is found showing the matched pod name and label
4. If pod is still empty after both selectors, show a `die` with instructions:
   `oc -n ${NS} get pods -l postgres-operator.crunchydata.com/cluster=${CLUSTER_NAME}`

**Relevant Context:**
- File: `scripts/10b_provision_pg_reporting.sh` lines 237–258
- Crunchy v5 changed role label: `master` → `primary`

---

### Sub-Task 2 — Fix undefined `${SVC}` variable in external DSD description

**Status:** `[x] done`

**Intent:** Line 569 uses `${SVC}` in a here-doc/string, which is never defined anywhere. The 
correct variable is `${PG_SVC}` (defined line 164 as `${CLUSTER_NAME}-rw`). This produces a 
broken port-forward example in the DSD description field.

**Expected Outcomes:**
- External DSD description contains the correct service name in the port-forward example.

**Todo List:**
1. Replace `svc/${SVC}` → `svc/${PG_SVC}` on line 569.

**Relevant Context:**
- `PG_SVC` defined line 164: `PG_SVC="${CLUSTER_NAME}-rw"`
- Bug is in the external DSD payload JSON string (Step 5 optional DSD section)

---

### Sub-Task 3 — Fix password desync with `--skip-postgres`

**Status:** `[x] done`

**Intent:** Line 269 generates a new random password unconditionally at script start. When 
`--skip-postgres` is passed, Step 1 (which sets the password in the DB) is skipped, but Step 2 
(K8s Secret) still writes the **new** random password. This causes a credential mismatch: the 
Secret has the new password but the DB has the old one — breaking all downstream connections.

**Expected Outcomes:**
- When `--skip-postgres` is used, the new password is only written to the Secret; no mismatch.
- OR: when `--skip-postgres` is used, the password is read back from the existing Secret instead 
  of generating a new one, so the DB credentials remain consistent.
- Debug output shows where the password came from (generated vs. read from Secret).

**Todo List:**
1. Move password generation into the `if ! $SKIP_PG` block (Step 1 section).
2. Before generating a new password, check if the Secret already exists and read the 
   existing password from it: `oc -n ${NS} get secret ${SECRET_NAME} -o jsonpath=...`
3. Only generate a new password if the Secret does not exist or contains the placeholder value.
4. Add `info` log: "Password: read from existing Secret" vs. "Password: newly generated"

**Relevant Context:**
- Password generation: line 269 (`REPORT_PASS=...`)
- Step 1 (DB provision): lines 284–327
- Step 2 (Secret write): lines 332–353

---

### Sub-Task 4 — Add `--max-time` to all curl calls and capture HTTP status codes

**Status:** `[x] done`

**Intent:** Five curl calls (role grant lookup, connection create, connection lookup, DSD create, 
external DSD create) have no `--max-time`. If the CPD host is unreachable or slow, the script 
hangs indefinitely. Additionally, several calls do not check the HTTP status code, so 4xx/5xx 
responses are treated as success.

**Expected Outcomes:**
- Every curl call has `--max-time 30` (30 s for the main API calls).
- Every curl call extracts the HTTP status code using `-w "\nHTTP_CODE:%{http_code}"`.
- A helper function `cpd_curl` wraps common logic: max-time, auth header, Content-Type, 
  status check, and emits `debug` with method + URL + HTTP status.
- On non-2xx responses the body is shown in the error message.

**Todo List:**
1. Create a `cpd_curl()` helper function that:
   - Accepts: METHOD URL PAYLOAD (payload optional)
   - Adds `-sk --max-time 30 -w "\nHTTP_CODE:%{http_code}"` automatically
   - Adds `-H "Authorization: Bearer ${TOKEN}"` and `-H "Content-Type: application/json"`
   - Returns the response body and sets a global `_CURL_HTTP` variable with the status code
   - Emits `debug "→ ${METHOD} ${URL} → HTTP ${_CURL_HTTP}"`
2. Replace the curl calls in Step 3b (role grant fetch + PUT), Step 4 (connection), 
   Step 5 (DSD, external DSD) with `cpd_curl`.
3. After each `cpd_curl` call, check `_CURL_HTTP` and emit appropriate `ok`, `warn`, or `die`.

**Relevant Context:**
- Step 3b: lines 423–450
- Step 4: lines 482–515
- Step 5: lines 544–583

---

### Sub-Task 5 — Fix `pg_sql` error capture and add SQL debug output

**Status:** `[x] done`

**Intent:** The `pg_sql` helper runs SQL via `oc exec` but its output is not checked after 
each call (lines 293, 306, 312). If a SQL statement fails (permissions, syntax, timeout), the 
error is printed to stdout/stderr but the script continues. The next step then operates on a 
broken database state.

**Expected Outcomes:**
- `pg_sql` returns non-zero on psql errors (already configured via `ON_ERROR_STOP=1`).
- Callers wrap `pg_sql` calls with `|| die "SQL failed: <context>"`.
- Successful SQL calls emit `debug` with a short summary of what ran.
- The smoke-test failure (line 325) is upgraded from `warn` to a more informative message 
  showing the full smoke output.

**Todo List:**
1. After each `pg_sql` call in Step 1, add `|| die "psql failed at <step description>"`.
2. Add a `debug` log before each `pg_sql` call showing the database and a one-line SQL summary.
3. Capture and display the smoke-test output more clearly — show user, database, schema.

**Relevant Context:**
- `pg_sql` definition: lines 275–279
- User creation call: line 293
- Database creation call: line 306
- Schema creation call: line 312
- Smoke test: lines 319–325

---

### Sub-Task 6 — Fix `die()` multi-arg formatting and add `debug` log level

**Status:** `[x] done`

**Intent:** The `die()` function is called with multiple string arguments using `\n` literal 
escapes (line 379–383), but `$*` joins all args with spaces — the `\n` becomes literal `\n` 
text rather than newlines when concatenated. Also, there is no `debug` log level for verbose 
tracing.

**Expected Outcomes:**
- Multi-line `die()` calls format correctly with newlines.
- A `debug()` helper function emits `[DEBUG]` lines when `VERBOSE=true` (set via `--verbose` flag).
- A `--verbose` option is added to the argument parser.

**Todo List:**
1. Fix `die()` to join its args with `\n` rather than space, or change multi-arg calls to 
   single-arg with embedded `\n`.
2. Add `debug()` helper: `debug() { $VERBOSE && echo -e "${DIM}[DEBUG]${RESET} $*"; }` 
3. Add `VERBOSE=false` default and `--verbose` option in the argument parser.
4. Add `debug` calls at: token fetch, curl responses, pod selector, SQL steps, .env writes.

**Relevant Context:**
- `die()` definition: line 128
- Multi-arg `die` call: lines 379–383
- Colour/helper block: lines 122–129

---

### Sub-Task 7 — Add pre-flight check for `python3` and fix macOS `sed -i`

**Status:** `[x] done`

**Intent:** `python3` is used throughout but never checked for existence in the pre-flight block 
(unlike `oc`). If missing, all JSON parsing silently fails. Also, `sed -i.bak` on macOS (BSD sed) 
requires a different syntax than GNU sed — `sed -i.bak` on macOS requires `sed -i '.bak'` or 
`sed -i '' ...`.

**Expected Outcomes:**
- Pre-flight block checks `python3` is available and dies with a helpful message if not.
- `_set_env()` works on both Linux (GNU sed) and macOS (BSD sed).
- python3 JSON parsing failures emit a `warn` with the raw response snippet (first 200 chars).

**Todo List:**
1. Add `command -v python3 &>/dev/null || die "python3 not found — install Python 3."` to the 
   pre-flight block (after the `oc` check).
2. In `_set_env()`, detect macOS (`uname -s`) and use the correct `sed -i ''` syntax.
3. In all python3 parsing calls, capture stderr and emit a `warn` (or `debug` in verbose mode) 
   when parsing fails, showing the first 200 characters of the raw response.

**Relevant Context:**
- Pre-flight block: lines 173–186
- `_set_env` function: lines 592–599
- python3 calls: lines 371, 377, 395, 425, 489, 494, 502, 550, 556, 578

---

### Sub-Task 8 — Fix `CURRENT_ROLES` empty-on-failure in role grant step

**Status:** `[x] done`

**Intent:** In Step 3b, `CURRENT_ROLES` is built by piping a curl call into python3. If the curl 
fails (network error, 401, 500) or python3 parses nothing, `CURRENT_ROLES` is empty. The script 
then sends `{"user_roles": ""}` (invalid JSON) to the PUT endpoint, silently failing to grant 
the role.

**Expected Outcomes:**
- If the GET user call fails or returns no roles, a clear `warn` is emitted showing the HTTP code.
- A safe default is used: if roles cannot be read, the role list is set to 
  `["wkc_reporting_administrator"]` with a warning that existing roles could not be preserved.
- The PUT request always sends valid JSON.

**Todo List:**
1. Use the new `cpd_curl()` helper for the GET user roles call (from Sub-Task 4).
2. Check `_CURL_HTTP` — if not 200, warn and use the safe default role list.
3. Validate `CURRENT_ROLES` is non-empty before the PUT; if empty, use safe default.
4. Show the final role list being sent to the PUT in a `debug` or `info` log.

**Relevant Context:**
- Step 3b: lines 422–451

---

## Implementation Notes

- Sub-Tasks 1–3 fix correctness bugs; implement first.
- Sub-Tasks 4–5 improve error capture; implement second.  
- Sub-Tasks 6–8 add observability and pre-flight hardening; implement third.
- Each sub-task touches a distinct section of the script and can be implemented independently.
- Do NOT change the overall step structure, option names, or script semantics.
- After all sub-tasks, run `bash --norc -n scripts/10b_provision_pg_reporting.sh` (syntax check) 
  and `shellcheck -S warning scripts/10b_provision_pg_reporting.sh` if shellcheck is available.

## Completion

All 8 sub-tasks implemented in a single rewrite of `scripts/10b_provision_pg_reporting.sh`.  
Validation results:
- `bash --norc -n scripts/10b_provision_pg_reporting.sh` → **EXIT:0** (no syntax errors)
- `shellcheck -S warning scripts/10b_provision_pg_reporting.sh` → **EXIT:0** (no warnings)
