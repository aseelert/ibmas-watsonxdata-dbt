# dbt run findings (2026-10-15)

A full dbt run from a re-provisioned cluster, and what went wrong on the way.
Read this if `bin/demo setup` or `bin/demo dbt debug` fails right after the
watsonx.data instance was rebuilt.

## The correct order

1. Export the Presto connection JSON from the watsonx.data UI and save it as `watsonx_data/instance_details.json`.
2. `cp .env.example .env` and set only `WXD_OC_PASSWORD`.
3. `bin/demo setup` fills in everything else and fetches the API key and bearer token.
4. `bin/demo dbt debug` should end with `All checks passed!`.
5. `bin/demo dbt seed` loads the 4 CSVs.
6. `bin/demo dbt build` builds Bronze → Silver → Gold and runs the tests.

Result of the run: **17 models/seeds succeeded, 34 tests passed, 0 failures** (about 49 s of dbt time).

## Issues found

| # | Symptom | Cause | Fix |
|---|---------|-------|-----|
| 1 | `dbt debug`: `503 service unavailable` | The old Presto engine (`presto653`) no longer existed; the new one was `presto83` | Re-export the connection JSON from the UI |
| 2 | `401 … unable to find instance with given instance id` | The watsonx.data instance ID changed along with the engine | Same re-export; the JSON carries the new `instance_id`. **Now detected:** see the checks below |
| 3 | `bin/demo setup` silently put the old host and instance ID back into `.env` | `watsonx_data/instance_details.json` was stale, and setup trusts it. Hand-editing `.env` is undone on the next setup | Fix the JSON, never `.env` by hand |
| 4 | `Password login failed (401)` while fetching tokens | `WXD_CPD_PASSWORD` was the `replace-with-…` placeholder. The script treats a placeholder as "already set" and skips the secret lookup, even with `--overwrite` | **Fixed:** a placeholder now counts as unset, so setup reads the password from the `platform-auth-idp-credentials` secret itself. If `.env` holds a different, real value, setup warns and `--overwrite` refreshes it |
| 5 | `dbt build` took about 7 minutes after finishing | With `OPENLINEAGE_URL` set and Marquez not running, each of the 46 lineage events retried a dead port for ~9 s. dbt itself had succeeded | `scripts/02_dbt_env.sh` now checks the URL first, warns and skips the emit |
| 6 | Nothing told you Docker was needed for lineage or Metabase | `bin/demo setup` never looked at Docker | New soft check, see below |

## New: Docker hint in `bin/demo setup`

Step 5b checks that a Docker runtime is up. It is a hint only:

- If Docker is running, it logs a one-line reminder of how to start the optional stacks.
- If Docker is missing or stopped, it prints a `WARNING` block, **waits 5 seconds** so you (or an agent reading the log) notice, then carries on. It never fails the setup.
- Set `WXD_SKIP_DOCKER_CHECK=1` to silence it.

Docker is needed only for Metabase, Marquez lineage, Airflow, OpenMetadata and the Confluent stack.
dbt, Spark and ingestion run without it.

## Notes for next time

- After any cluster or instance rebuild, **re-export the JSON first**; everything downstream depends on it.
- `bin/demo setup` also runs `oc login` and renames the oc context to `admin`. This is expected.
- A dbt exit code of 0 with OpenLineage tracebacks above it means the build worked and only the lineage emit failed.

## What `bin/demo setup` and `bin/demo dbt` now check

Each check is a warning, not an error, and the ones that matter pause for 5 seconds so a person or an agent notices.

| Situation | Behavior |
|-----------|----------|
| `watsonx_data/instance_details.json` missing | Stops with the exact UI path to export it. `.env` is not touched. In a terminal, an empty `.env` starts a guided wizard instead |
| JSON present but stale (Presto engine host/ID or instance ID differs from the live cluster) | Warns, shows JSON vs live values, pauses 5 s and advises `bin/demo setup --overwrite` (or a re-export). With `--overwrite` the **live cluster values replace the stale JSON values automatically**. Needs an `oc` session |
| `.env` missing, empty or copied from `.env.example` | Setup fills what it can. A placeholder `WXD_OC_PASSWORD` is reported clearly instead of failing inside `oc login` |
| `WXD_CPD_PASSWORD` placeholder or different from the cluster secret | Read from the cluster; a changed value is flagged |
| `cpadmin` cannot log in (401) | If `oc` is logged in and the cluster secret holds a different password, setup **retries with it automatically** and fixes `.env`. Otherwise it explains the likely causes (stale password, wrong JSON, wrong user), advises `bin/demo setup --overwrite`, and pauses 5 s |
| `WXD_HOST`, `WXD_INSTANCE_ID` or `WXD_API_KEY` empty or placeholder when running `bin/demo dbt …` | Warns with the fix (`bin/demo setup`), pauses 5 s |
| `certs/watsonxdata-ca.pem` missing when running dbt | Warns, pauses 5 s |
| Docker not running | Hint only, see above |
| `--dry-run` and the end-of-setup summary | Passwords, tokens and keys are masked (they used to be printed, including the start of the bearer token) |
| JSON missing but `oc` is logged in | `bin/demo setup` **builds the JSON from the cluster** (engine route, instance ID, CA chain) and tells you it did; re-export from the UI any time to replace it |
| `bin/demo spark / ingest / query / validate / reset / streaming` | Same `.env`, JSON and certificate checks run first; a warning pauses 5 s, then the command continues |
| `bin/demo streaming / metabase / airflow / lineage / catalog / docker build / docker start` | Docker hint first (warn, 5 s, continue) |
| `.env` and the JSON disagree (instance or host) | Detected offline by `bin/demo doctor` and the preflight, with the fix `bin/demo setup --overwrite` |

Silence all `bin/demo` preflights with `WXD_SKIP_PREFLIGHT=1`.

## `bin/demo doctor`

Read-only health check you can run any time, or hand to an agent before it starts work:

```bash
bin/demo doctor
```

It checks `.env` values, the connection JSON (present, and consistent with `.env`), the CA certificate, how long the token is still valid, and whether Docker is running. Each problem prints one line with the exact command that fixes it. It never changes anything.

## One more thing to know

Every `bin/demo setup` run asks CPD for an API key. Running setup against a *different* env file (for example a test copy) can invalidate the key in your real `.env`, and `dbt debug` then fails with a 401. Run `bin/demo setup` again on the real `.env` to fix it.

## Governance provisioning: "nothing shows up" (2026-10-15)

`bin/demo governance` imported and published everything (15 terms, 15 data classes, 2 classifications, 6 categories, 1 masking rule), yet the CPD UI and `--verify-only` showed 0 artifacts.

| Finding | Detail |
|---------|--------|
| Not an API change | The import and publish calls return success and the artifacts are `PUBLISHED` when read from `/v3/glossary_terms`, `/v3/data_classes` and `/v3/classifications` |
| Search index is missing them | `/v3/search` returns only the data protection rule. Even the 165 built-in data classes that this repo never touched are missing. The index that feeds search and the UI lists is not being populated for glossary artifacts, which is a cluster-side indexing problem |
| The script hid it | Its verify step and its draft detector both read the index, so a count of 0 still ended in "imported, published and verified" |
| Fixed | Verify now reads the direct artifact endpoints and fails when an expected artifact is not published. When the artifact API and the search index disagree, it prints a `WARNING` and a different final message. `--verify-only` shows both counts |
| Categories | `GET /v3/categories` (the list) answers 403 even for `cpadmin`, who does have `manage_categories`. Reading one category by ID works. The script now follows each term, classification and data class up its `parent_category` chain and reads every parent by ID, which confirmed all 6 categories as PUBLISHED. A category with nothing below it cannot be seen this way and is reported as unconfirmed, not as a failure |
| Edition | This cluster runs `wkc` 5.3.23 (Enterprise licence) plus both `ikc-premium` and `ikc-standard`. The `/v3` governance endpoints used here behave the same on it; the differences are in which list calls the user may make and in search indexing, not in the URL paths |

If the UI stays empty while `bin/demo governance --verify-only` shows `published(api)` complete, the cluster's governance search indexing needs a repair or re-index. That is a cluster action, not a script fix.

### Why a category created in the UI behaved differently

Creating a category in the UI also indexes it straight away (`PUT /v3/search/bulk_request`) and adds two access rules: the creator as owner and **all users as viewer**. The CSV import only adds the owner, so any other user sees none of the imported categories. `bin/demo governance` now finishes with a "share categories" step that adds the all-users viewer rule (`POST /v3/categories/{id}/collaborators` with `{"role": "viewer", "principal_id": "10000", "user_type": "GROUP"}`) to each category it provisioned. Re-running is safe: categories that already have the rule are left alone.

## CPD fabric: project, connection, DSD, import and enrichment (`bin/demo ikc`)

### Object names (all lower case, set in `.env`)

| Object | Default name | `.env` variable |
|---|---|---|
| Project | `ibmas-dlh-medallion-project` | `WXD_CPD_PROJECT` |
| Catalog | `ibmas-dlh-medallion-catalog` | `WXD_CPD_CATALOG` (not created yet, see below) |
| Connection | `ibmas-watsonx-data-presto-con` | `WXD_CPD_CONNECTION` |
| Data source definition | `ibmas-watsonx-data-presto-dsd` | `WXD_CPD_DSD` |
| Metadata import | `ibmas-dlh-medallion-mdi` | `WXD_CPD_MDI` |
| Metadata enrichment | `ibmas-dlh-medallion-mde` | `WXD_CPD_MDE` |
| Postgres reporting connection / DSD | `ibmas-reporting-postgres-con` / `ibmas-reporting-postgres-dsd` | `WXD_CPD_PG_CONNECTION` / `WXD_CPD_PG_DSD` |

### What the script had to get right (found by testing against a rebuilt cluster)

| Finding | Detail |
|---|---|
| Connection needs `origin_country` at creation | Setting it afterwards does not satisfy the metadata-import service (it answers "No origin country value was specified for the connection"). The script creates connections with `origin_country: us` |
| Saved credentials go stale | Every `bin/demo setup` fetches a new API key, so the key stored inside a connection stops working and any update to the connection fails with "Unauthorized". The script refreshes the key in the connection on every run |
| Rename, never recreate | Recreating a connection orphans what was imported through it. Old names (`ibmas-ingest-demo`, `ibmas-presto`) are renamed in place: `PATCH /v2/projects/{id}`, `PATCH /v2/connections/{id}` (JSON Patch), and `PATCH /v2/assets/{id}` for DSDs |
| One DSD per endpoint | CPD rejects a second DSD on the same host and port ("These endpoints already exist in another data source definition asset") |
| Enrichment payload | The service rejects a minimal body with HTTP 400 and an empty error list. It needs the full default structure (sampling, data-quality checks, quality origins). The script sends it |
| DataStage IDs | `scripts/datastage/create_medallion_flows_v2.py` hardcoded the old cluster's project and connection IDs. It now looks them up by name |

### Tested

A full create-from-nothing run in a throwaway namespace of test names created the project, both connections, the import (completed) and the enrichment (completed) in about 10 minutes. The test objects were deleted afterwards.

### Open

* **Data classes are not assigned yet.** The enrichment profiled the Gold tables but the `email` column shows `NoClassDetected`, so the `RML` data classes are not being applied. Not yet diagnosed.
* **Catalog `ibmas-dlh-medallion-catalog`** is not created: the catalog API asks for a storage bucket and the two existing catalogs have none.
* **Postgres connection** needs a Postgres. This cluster has only EDB CloudNativePG, not the Crunchy operator the PG script expects.
