# Scripts Reference

All scripts live in `scripts/`. Run from the **repo root** with the virtualenv active:

```bash
source .venv/bin/activate
```

---

## Quick-copy: full demo run order

```bash
# ── ONCE: import connection JSON → .env + SSL cert ───────────────────────────
python scripts/prepare_watsonx_env.py
# then open .env and set WXD_API_KEY=<your-key>

# ── EVERY SESSION: validate auth ─────────────────────────────────────────────
python scripts/get_token.py --export     # checks key, writes bearer token to .env

# ── ONCE on fresh env: create schemas ────────────────────────────────────────
python scripts/bootstrap_watsonxdata.py

# ── PATH A · dbt ─────────────────────────────────────────────────────────────
bash scripts/dbt_env.sh seed
bash scripts/dbt_env.sh run
bash scripts/dbt_env.sh test

# ── PATH B · Spark ───────────────────────────────────────────────────────────
# terminal 2 (keep open while uploading):
oc -n cpd-instance port-forward svc/ibm-lh-lakehouse-minio-svc 19000:9000

python scripts/upload_spark_assets.py
WXD_SPARK_DRY_RUN=false python scripts/submit_spark_application.py
python scripts/spark_application_status.py   # repeat until finished

# ── PATH C · cpdctl ingestion ────────────────────────────────────────────────
python scripts/ingest_with_cpdctl.py --wait          # submit + poll to completion
python scripts/ingest_with_cpdctl.py --status --batch <id>   # check a prior run

# ── PATH D · Confluent streaming (Kafka→Flink→Iceberg) ───────────────────────
python scripts/check_hosts.py                        # verify /etc/hosts → bastion
bash confluent/scripts/expose_minio_route.sh         # ONCE: expose real MinIO to Docker (writes WXD_OBJECT_STORE_ENDPOINT)
bash confluent/start.sh --all                        # local stack + 8 topics + produce 1,704 seed rows to Kafka
bash confluent/start.sh --silver                     # submit 9 Flink jobs → confluent_demo_silver + register_table
# (if you re-run --silver from the host, Phase B re-registers the tables in watsonx.data)
python confluent/scripts/submit_confluent_gold.py --no-dry-run --wait   # build confluent_demo_gold (Spark TABLE) — auto-creates the 2 Presto VIEWS
# or, with CONFLUENT_GOLD_ENGINE=datastage:
bash confluent/start.sh --gold --engine datastage    # build the same gold via DataStage
# (standalone: rebuild just the two Presto VIEW marts for either path)
python scripts/create_gold_views.py --path confluent

# ── VERIFY 3-way parity (dbt == Spark == Confluent) ──────────────────────────
python scripts/reconcile_gold.py                     # gold = 494 / 5 / 50, identical across engines

# ── QUERY gold layer ─────────────────────────────────────────────────────────
python scripts/query_gold.py

# ── OPTIONAL: Iceberg time travel demo ───────────────────────────────────────
python scripts/demo_time_travel.py

# ── OPTIONAL: OpenMetadata artifacts ─────────────────────────────────────────
python scripts/prepare_openmetadata_dbt_artifacts.py
python scripts/upload_dbt_artifacts.py

# ── TEARDOWN (destructive!) ───────────────────────────────────────────────────
scripts/reset_demo.sh --all --dry-run   # preview everything that would be removed
scripts/reset_demo.sh --all             # Docker stacks + schemas + MinIO files
# (or just the schemas, like before:)
python scripts/cleanup_watsonxdata.py
```

---

## Step-by-step reference

### 0a · `prepare_watsonx_env.py` — import connection JSON

**Run once** after downloading the Presto connection JSON from the watsonx.data console
(**Engine details → Download connection**). Save the file as `watsonx_data/instance_details.json`, then:

```bash
python scripts/prepare_watsonx_env.py
```

Writes `WXD_HOST`, `WXD_PORT`, `WXD_INSTANCE_ID`, `WXD_CPD_HOST`, `WXD_CPD_AUTH_URL`,
and the SSL cert to `certs/watsonxdata-ca.pem`. Preserves any existing `WXD_API_KEY`.

| Flag | Default | Description |
|---|---|---|
| `--connection-json PATH` | `watsonx_data/instance_details.json` | Downloaded JSON |
| `--env-file PATH` | `.env` | Target env file |
| `--cert-file PATH` | `certs/watsonxdata-ca.pem` | PEM output path |
| `--overwrite` | off | Re-import non-secret values even if already set |

---

### 0b · `get_token.py` — validate auth + refresh API key

**Run at the start of every session.** Use `--export` so the bearer token is written to `.env`
before running Spark (Spark submission reads `WXD_SPARK_BEARER_TOKEN`).

```bash
python scripts/get_token.py --export
```

If the API key is expired the script falls back to password login, regenerates a fresh key,
saves it to `.env`, and continues — no manual copy-paste needed.

```bash
# Force password login + regenerate key (e.g. after a long break)
python scripts/get_token.py --refresh-key --export
```

| Flag | Description |
|---|---|
| `--export` | Write bearer token to `WXD_SPARK_BEARER_TOKEN` in `.env` |
| `--refresh-key` | Skip API key check, login with password, regenerate and save new key |
| `--env-file PATH` | Override `.env` path |

??? tip "Getting an API key from the UI"
    1. Open `https://<WXD_CPD_HOST>` and log in as `cpadmin`.
    2. Avatar (top-right) → **Profile and settings** → **API key** tab.
    3. **Regenerate API key** → copy the value.
    4. Paste into `.env`: `WXD_API_KEY=<new-key>`.

---

### 1 · `bootstrap_watsonxdata.py` — create demo schemas

**Run once on a fresh environment**, before dbt or Spark.

```bash
python scripts/bootstrap_watsonxdata.py
```

Creates `dbt_demo_raw`, `dbt_demo_bronze`, `dbt_demo_silver`,
`dbt_demo_gold` in the `iceberg_data` catalog.

---

### 2 · `dbt_env.sh` — dbt with `.env` loaded

Wrapper that sources `.env` before calling dbt — use this instead of calling `dbt` directly.

```bash
bash scripts/dbt_env.sh seed              # load CSV seeds → raw tables
bash scripts/dbt_env.sh run               # build bronze → silver → gold
bash scripts/dbt_env.sh test              # run data quality tests

bash scripts/dbt_env.sh run --select silver_orders   # single model
bash scripts/dbt_env.sh run --full-refresh            # force full rebuild
bash scripts/dbt_env.sh docs generate && bash scripts/dbt_env.sh docs serve
```

---

### 3 · Spark path

#### `upload_spark_assets.py` — push app + CSVs to MinIO

Run **once** before submitting the Spark job. MinIO has no external route,
so open a port-forward first (keep it open in a second terminal):

```bash
oc -n cpd-instance port-forward svc/ibm-lh-lakehouse-minio-svc 19000:9000
```

```bash
python scripts/upload_spark_assets.py
```

Uploads `spark/app/load_medallion_demo.py` and all seed CSVs to `s3a://iceberg-bucket/spark_demo/`.

---

#### `submit_spark_application.py` — submit the Spark job

Requires `WXD_SPARK_BEARER_TOKEN` in `.env` — run `get_token.py --export` first.

```bash
# Dry run (default) — prints request body, does not submit
python scripts/submit_spark_application.py

# Real submission
WXD_SPARK_DRY_RUN=false python scripts/submit_spark_application.py
```

Prints the application ID on success. Copy it to `.env` as `WXD_SPARK_APPLICATION_ID`.

---

#### `spark_application_status.py` — check job status

```bash
python scripts/spark_application_status.py               # reads WXD_SPARK_APPLICATION_ID
python scripts/spark_application_status.py <app-id>      # or pass directly
```

Returns `running`, `finished`, or `failed`.

---

### 4 · `ingest_with_cpdctl.py` — native CSV ingestion

Requires `cpdctl` installed and on `PATH`. You do **not** need to log cpdctl in by
hand: every run validates `WXD_API_KEY` against CPD and re-syncs cpdctl's cached
credentials from `.env` first, so the stale-cache
`authenticate step: Unauthorized` error can't recur. If the key itself is rejected
you get a clear 401 pointing you to `python scripts/get_token.py --refresh-key`.

```bash
python scripts/ingest_with_cpdctl.py                  # submit all four jobs
python scripts/ingest_with_cpdctl.py --wait           # submit, then poll to completion
python scripts/ingest_with_cpdctl.py --status --batch <id>         # check a prior run
python scripts/ingest_with_cpdctl.py --status --batch <id> --wait  # poll a prior run
```

Reads CSVs from `s3a://iceberg-bucket/spark_demo/raw/` and ingests them into
`spark_demo_cpdctl_raw` via the watsonx.data native ingestion engine.

| Flag | Default | What it does |
|------|---------|--------------|
| `--wait` | off | After submitting, poll all jobs until they reach a terminal state. |
| `--status` | off | Skip submission; just report the status of an existing batch's jobs. |
| `--batch <id>` | timestamp | Batch id to target. Each run prints its id; pass it back to `--status`. |
| `--interval <s>` | `20` | Seconds between polls when waiting. |
| `--timeout <s>` | `900` | Max seconds to wait before giving up. |

Job ids are deterministic (`ingest-<table>-<batch>`), so `--status` needs only the
batch id — no state file. Status is read via `cpdctl wx-data ingestion get`.

---

### 5 · `query_gold.py` — query the gold layer

```bash
python scripts/query_gold.py              # all reports
python scripts/query_gold.py daily_sales
python scripts/query_gold.py customer_360
```

Connects to Presto and prints formatted tables. Requires a valid API key in `.env`
(run `get_token.py --export` first if you haven't this session).

---

### 6 · `demo_time_travel.py` — Iceberg time travel

```bash
python scripts/demo_time_travel.py
```

Shows snapshot history, partition metadata, and a time-travel query against the silver layer.
Requires the silver schema to exist (run dbt or Spark path first).

---

### 7 · OpenMetadata artifacts

#### `prepare_openmetadata_dbt_artifacts.py`

```bash
python scripts/prepare_openmetadata_dbt_artifacts.py            # full run: seed+run+test+docs
python scripts/prepare_openmetadata_dbt_artifacts.py --docs-only # lineage only: docs generate + stage
python scripts/prepare_openmetadata_dbt_artifacts.py --skip-dbt  # use existing target/*.json
python scripts/prepare_openmetadata_dbt_artifacts.py --skip-seed # skip seed step only
```

By default the script runs `dbt seed --full-refresh`, `dbt run`, `dbt test`, and
`dbt docs generate`, then stages `manifest.json`, `catalog.json`, and `run_results.json`
into `openmetadata/dbt-artifacts/`.

| Flag | Default | What it does |
|------|---------|--------------|
| `--docs-only` | off | Lineage-only mode: run **only** `dbt docs generate` (no seed/run/test), then stage. Requires the medallion tables to already exist. Mutually exclusive with `--skip-dbt`. |
| `--skip-dbt` | off | Only copy existing `target/*.json` artifacts; do not run any dbt commands. |
| `--skip-seed` | off | Skip the `dbt seed` step but still run `dbt run`, `dbt test`, and `dbt docs generate`. |
| `--artifact-dir <path>` | `openmetadata/dbt-artifacts/` | Directory where the staged artifacts are written. |
| `--retries <n>` | `1` | Number of retries for each dbt command. |

---

#### `generate_lineage_docs.sh`

```bash
scripts/generate_lineage_docs.sh                 # refresh lineage artifacts only
scripts/generate_lineage_docs.sh --retries 3     # flags forwarded to the python stager
```

A thin, lineage-only convenience wrapper around
`prepare_openmetadata_dbt_artifacts.py --docs-only`. It runs **only**
`dbt docs generate` (no seed/run/test) and stages the three artifacts OpenMetadata
reads. Use it when the medallion tables are already built and you just want fresh
lineage/column metadata for the catalogue. All flags are forwarded verbatim to the
python stager (e.g. `--artifact-dir`, `--retries`); do not pass `--skip-dbt`.

---

#### `upload_dbt_artifacts.py`

```bash
python scripts/upload_dbt_artifacts.py
```

Pushes staged artifacts to `s3://iceberg-bucket/openmetadata/dbt-artifacts/dbt_demo/`
so OpenMetadata can read them during ingestion.

#### `apply_openmetadata_governance.py`

```bash
python scripts/apply_openmetadata_governance.py --mode online
python scripts/apply_openmetadata_governance.py --mode offline --strict
```

Applies the OpenMetadata governance layer after table and dbt lineage ingestion:
`MedallionGlossary`, `MedallionLayer`, `DemoDataDomain`, and
`MetadataIngestionMode`. The main ingestion script runs it automatically. Use this
manual command only when you want to re-apply glossary terms, descriptions, or
auto-classification tags without re-running the full ingestion.

---

## Confluent streaming scripts

These scripts drive the streaming path (Path D in the docs) — Kafka → Flink → Schema Registry →
Iceberg → watsonx.data — plus the gold build and the 3-way parity check. See the
[Confluent walkthrough](confluent-demo.md) and the [DataStage page](datastage-demo.md) for the
full story. All hosts, schemas, and engine choices come from `.env` — nothing is hardcoded.

!!! info "Where each script lives"
    The orchestrator and its helpers live under **`confluent/`**
    (`confluent/start.sh`, `confluent/scripts/*`). Three scripts that are **shared** with the
    Spark path live under **`scripts/`** (`scripts/create_gold_views.py`,
    `scripts/reconcile_gold.py`, `scripts/check_hosts.py`). Paths below are exact — copy them
    verbatim.

!!! note "Env toggles for the gold build"
    * `CONFLUENT_GOLD_ENGINE` — `spark` (default) or `datastage`; picks the gold engine.
    * `CONFLUENT_GOLD_S3_BRIDGE` — **legacy, default off** (`0`). The durable `s3a://` fix is
      live, so leave it off; set `1` only as a fallback for old `s3://`-pathed tables.
    * `WXD_SPARK_CREATE_VIEWS` — **default on** (`true`). After a Spark gold app finishes, the
      two Presto VIEW marts are created automatically; set `false` for table-only behaviour.
      The Confluent submitter has its own `--no-views` flag for the same effect.

### `check_hosts.py` — validate `/etc/hosts` for the cluster

```bash
python scripts/check_hosts.py
```

The cluster's wildcard DNS (`*.apps.watson.ibmas-zocp-techcluster.org`) is **not** on the public
internet — every hostname must resolve to the bastion `9.82.206.23` via `/etc/hosts`. This script
checks all required entries at once: that each hostname is present, resolves to the expected
bastion IP, **and** is TCP-reachable on its port (`6443` for the API, `443` for the Routes). It
prints the exact lines to add (with a copy-paste `sudo tee` block) for anything missing, and exits
`0` only when every entry passes. Run it before the Spark and Confluent paths.

### `confluent/start.sh` — the one entrypoint (subcommand-driven)

```bash
bash confluent/start.sh                       # default = --all
bash confluent/start.sh --all                 # full local bring-up + seed Kafka
bash confluent/start.sh --silver              # submit the 9 Flink jobs → silver + register
bash confluent/start.sh --gold --engine spark # build confluent_demo_gold (or --engine datastage)
bash confluent/start.sh --status              # health + per-topic counts + UI URLs (read-only)
bash confluent/start.sh --stop                # stop the 7 containers, keep data/volumes
bash confluent/start.sh --reset -y            # DESTRUCTIVE wipe of the Confluent surface
```

The single, idempotent orchestrator for the whole streaming stack, run from the repo root. It
picks **one** action; the default is `--all`. Global options `-y/--yes` (no prompts),
`--dry-run` (preview only), and `-h/--help` apply to every action.

| Subcommand | What it does |
|---|---|
| `--all` *(default)* | Full local bring-up: ensure `.venv` + `requirements.txt`, build the `wxd-flink:1.20` image (skipped if cached), start the **7** long-running services (Kafka, Schema Registry, Kafbat UI, Iceberg REST, Flink JobManager + TaskManager + SQL Gateway), wait for Kafka health, create the **8** topics, produce all **1,704** seed rows, print a status summary. |
| `--stack` | Start **only** the 7 long-running containers (venv + image + services). No topics, no seeding. |
| `--silver` | Run the Flink silver pipeline via three `watsonxdata`-profile one-shots: `confluent-schema-prep` (Phase A — create the `confluent_demo_silver` + `confluent_demo_gold` schemas in watsonx.data), `confluent-flink-runner` (submit the 9 jobs in `silver_jobs.sql`), and `confluent-prep` (Phase B — `register_table` the 5 silver tables). **Requires a reachable MinIO Route + `WXD_OBJECT_STORE_ENDPOINT` in `.env`** — run `expose_minio_route.sh` first. |
| `--gold` | Build the `confluent_demo_gold` marts from silver. Uses `--engine` (`spark` default, or `datastage`); default comes from `CONFLUENT_GOLD_ENGINE`. |
| `--status` | Read-only: service health, per-topic message counts (via the Kafbat API), UI URLs. Safe anytime. |
| `--reset` | **Destructive.** Delegates to `scripts/reset_demo.sh --confluent`. |
| `--stop` | Stop the 7 containers, keep data/volumes. Restart with `--stack`. |

UIs after start: Kafbat `:28080`, Flink Web `:28085`, SQL Gateway `:28083`, Schema Registry
`:28081`, Iceberg REST `:28181`. Force a Flink rebuild with `docker rmi wxd-flink:1.20`.

!!! note "Topic count — 8, not 4"
    The stack creates **8 topics** (4 `raw_*` + 4 `silver_*`), driven by the subcommands above
    (`--all` seeds them, `--silver`/`--gold` run the pipeline). `sales_enriched` is **not** a
    topic — it is a Flink stream-stream join result written straight to Iceberg.

### `confluent/scripts/ingest_csv_to_kafka.py` — CSV → Kafka as governed Avro

```bash
.venv/bin/python confluent/scripts/ingest_csv_to_kafka.py
```

* **Purpose:** read the 4 seed CSVs and produce each row as an **Avro** message; the first message
  per topic auto-registers subject `<topic>-value` from `confluent/schemas/*.avsc`. The raw Kafka
  topics ARE the replayable "bronze".
* **When:** automatically, during `--all` (after the topics exist).
* **Key flags/env:** `--bootstrap-servers` (def `localhost:29092`), `--schema-registry-url`
  (def `http://localhost:28081`), `--csv-dir` (def `seeds/`), `--schemas-dir`
  (def `confluent/schemas/`). Type coercion is driven by the `.avsc` types; a mandatory-but-empty
  cell skips the row (data-quality).

!!! note "The loader produces Avro, not JSON"
    Each row is produced as **Avro** via `AvroSerializer` + Schema Registry (not JSON) — the
    Schema Registry holds the contract for every topic.

### `confluent/scripts/expose_minio_route.sh` — open a door to the real object store

```bash
bash confluent/scripts/expose_minio_route.sh
```

* **Purpose:** create an OpenShift edge-TLS **Route** to `ibm-lh-lakehouse-minio-svc` so the
  Docker containers can read/write the real `iceberg-bucket` without a port-forward tunnel; writes
  `WXD_OBJECT_STORE_ENDPOINT` into `.env`.
* **When:** **ONCE**, before submitting silver.
* **Key env:** `WXD_OPENSHIFT_API`, `WXD_OC_TOKEN` (or context / user+password),
  `WXD_OPENSHIFT_NAMESPACE` (def `cpd-instance`), `WXD_BASTION_IP`. Also updates `/etc/hosts`
  (sudo) and curls `/minio/health/live` to verify.

### `confluent/scripts/create-topics.sh` — make the 8 topics

```bash
# runs automatically; manual:
docker compose run --rm confluent-kafka-init
```

* **Purpose:** idempotently create 4 `raw_*` + 4 `silver_*` topics (partitions 1, replication 1).
  Auto-create is OFF on purpose, so all lanes are made up front.
* **When:** one-shot container `confluent-kafka-init`, after Kafka is healthy.

### `confluent/scripts/submit-flink.sh` — render placeholders + submit the 9 jobs

```bash
# runs inside the confluent-flink-runner container during --silver
```

* **Purpose:** substitute `${WXD_OBJECT_STORE_ENDPOINT}`, `${SCHEMA_REGISTRY_URL}`, and
  `${CONFLUENT_SILVER_SCHEMA}` into `silver_jobs.sql` at submit time, wait for the JobManager + SQL
  Gateway, **cancel any existing `kafka-raw-to-silver`/`kafka-silver-to-iceberg` jobs** (idempotent
  re-run guard, waits for terminal state), then submit via `sql-client.sh gateway`.
* **When:** automatically, inside `confluent-flink-runner` (`--silver`).
* **Notes:** fails loudly if any `${...}` placeholder survives or if `WXD_OBJECT_STORE_ENDPOINT` is
  empty. Injects the **in-container** Schema Registry URL `http://confluent-schema-registry:8081`
  (the host `localhost:28081` is unreachable from inside Docker).

### `confluent/scripts/prep_iceberg_schemas.py` — schemas + register (two phases)

```bash
python confluent/scripts/prep_iceberg_schemas.py --phase all
python confluent/scripts/prep_iceberg_schemas.py --phase schema     # Phase A only
python confluent/scripts/prep_iceberg_schemas.py --phase register   # Phase B only
```

* **Phase A (`--phase schema`):** create both `confluent_demo_silver` and `confluent_demo_gold`
  schemas in watsonx.data via Presto, **before** Flink writes. (Service `confluent-schema-prep`.)
* **Phase B (`--phase register`):** after Flink checkpoints, query the local Iceberg REST catalog
  for each silver table's current `metadata.json`, strip back to the table directory, and
  `CALL iceberg_data.system.register_table(...)` via Presto. Retries up to ~60s per table;
  idempotent (skips already-registered). (Service `confluent-prep`.)
* **Tables registered (5):** the four `confluent_silver_*` plus `confluent_silver_sales_enriched`.

### `confluent/scripts/submit_confluent_gold.py` — Confluent gold via Spark (+ Presto VIEWs)

```bash
python confluent/scripts/submit_confluent_gold.py                    # dry-run (default): prints redacted payload
python confluent/scripts/submit_confluent_gold.py --no-dry-run --wait
```

* **Purpose:** POST `confluent/spark/confluent_gold.py` to the watsonx.data Spark engine. The Spark
  app writes **only** `confluent_gold_daily_sales` (a TABLE). After the app **FINISHES**, this
  submitter runs `scripts/create_gold_views.py --path confluent` to create the two VIEW marts via
  Presto — the dbt-parity materialisation.
* **When:** after silver is registered, with the Spark engine running (`CONFLUENT_GOLD_ENGINE=spark`).
  Reuses the same Spark machinery/credentials as `submit_spark_application.py`.
* **Key flags:** defaults to **dry-run**; `--no-dry-run` to actually submit; `--wait` to poll to a
  terminal state; `--no-views` to skip the Presto VIEWs (restores old table-only/wait behaviour).
  Default sizing 1 core / 2G driver + executor.
* **Env:** the s3→s3a bridge is **disabled by default**; set `CONFLUENT_GOLD_S3_BRIDGE=1` only as a
  legacy fallback for old `s3://`-pathed tables.

!!! note "Why a second engine for gold?"
    Flink excels at per-row streaming transforms, but gold is **aggregation** over the whole silver
    set — a batch job. So Spark (or DataStage) reads the finished silver tables and builds the
    marts. Streaming silver + batch gold is the deliberate split.

### `confluent/scripts/create_datastage_flow.py` — no-code gold alternative

```bash
# requires CONFLUENT_GOLD_ENGINE=datastage in .env
python confluent/scripts/create_datastage_flow.py                    # dry-run (default): prints the request only
python confluent/scripts/create_datastage_flow.py --apply --run
```

* **Purpose:** when `CONFLUENT_GOLD_ENGINE=datastage`, author (and optionally run) a DataStage flow
  in CP4D project `ibmas-ingest-demo` that builds the **same** `confluent_demo_gold` marts. Loads a
  parameterized JSON template, substitutes env placeholders, and POSTs to the DataStage flows API.
* **When:** instead of the Spark gold job. **Needs a live CP4D cluster with the DataStage cartridge.**
* **Key flags:** default **dry-run** (prints the request); `--apply` to create the flow; `--run` to
  also compile + create a job + start a run. Needs `WXD_DATASTAGE_PROJECT_ID`,
  `WXD_DATASTAGE_CONNECTION_REF`. See the [DataStage page](datastage-demo.md) for prerequisites.

### `scripts/create_gold_views.py` — the two VIEW marts, via Presto (NOT Spark)

```bash
python scripts/create_gold_views.py --path confluent
python scripts/create_gold_views.py --path spark
```

* **Purpose:** create `*_gold_category_performance` and `*_gold_customer_360` as catalog **VIEWs**
  using the exact dbt SQL, for either `--path spark` or `--path confluent`. Idempotent: drops any
  pre-existing table OR view at each name first; never touches `gold_daily_sales`.
* **When:** automatically, after a Spark gold app finishes (invoked by `submit_confluent_gold.py`
  and by the Spark path's own submitter); run it standalone to rebuild just the two VIEWs.
* **Why via Presto:** a Spark `CREATE VIEW` produces a **Hive view** that watsonx PrestoDB refuses
  ("Hive views are not supported"). So Spark writes only the table; Presto owns the two views —
  exactly like dbt. On the Spark path this auto-run is gated by `WXD_SPARK_CREATE_VIEWS` (default on).

### `scripts/reconcile_gold.py` — prove 3-way gold parity

```bash
python scripts/reconcile_gold.py                       # all three paths
python scripts/reconcile_gold.py --paths dbt,confluent # any subset (min 2)
```

* **Purpose:** the verification finale for the whole repository. Symmetric `EXCEPT` (both
  directions) of the three canonical marts across **dbt / Spark / Confluent**; dbt is the reference
  when present. Read-only; prints a PASS/FAIL table; exit `0` = identical, `1` = a discrepancy.
* **When:** after all gold exists. The streaming-era successor to the
  [dbt-vs-Spark SQL comparison](sql-demo.md).
* **Expected parity:** gold = **494** `daily_sales` / **5** `category_performance` / **50**
  `customer_360`, identical across all three engines (silver counts are **50 / 20 / 500 / 1134 /
  1134**).

---

### 8 · `cleanup_watsonxdata.py` — drop the schemas

```bash
python scripts/cleanup_watsonxdata.py
```

Drops every demo schema and the tables/views inside them: `dbt_demo_{raw,bronze,silver,gold}`,
`spark_demo_{bronze,silver,gold}`, and the cpdctl raw schema (`WXD_INGEST_SCHEMA`,
default `spark_demo_cpdctl_raw`).

!!! danger "Destructive"
    Drops the catalog objects only. Iceberg **data files** may linger in object storage —
    use `cleanup_minio.py` (or the all-in-one `reset_demo.sh` below) to delete those too.

### 9 · `cleanup_minio.py` — delete the demo's MinIO files

```bash
python scripts/cleanup_minio.py --dry-run   # list what would be deleted
python scripts/cleanup_minio.py             # delete
```

Scoped deletion of only the demo's own prefixes inside `iceberg-bucket`: the medallion
schema folders (Iceberg table data at the bucket root), the `spark_demo/` asset prefix
(uploaded Spark app + raw CSVs), and `openmetadata/dbt-artifacts/`. It never empties the
whole bucket. Needs an `oc` session (MinIO has no external Route on this cluster).

### 10 · `configure_ikc_reporting.sh` — IKC reporting settings (optional, CPD only)

!!! info "Optional — requires CPD Enterprise + `oc` login"
    This script is only relevant when running against **IBM Software Hub (CPD)** with a
    `wkc-cr` Enterprise license.  It is **not** needed for the open-source paths (dbt / Spark /
    Confluent).  See [IKC Reporting — Optional Setup](ikc-reporting.md) for the full procedure.

Configure the `enforceAuthorizeReporting` / `defaultAuthorizeReporting` flags, patch the three
`wkc-cr` spec fields, restart the 7 required pods, and verify env-var propagation — all
in one idempotent command:

```bash
# Recommended for demos: default=true, enforce=false
bash scripts/configure_ikc_reporting.sh

# Lock all reporting on (enforce=true, default=true)
bash scripts/configure_ikc_reporting.sh --enforce

# Preview without changing anything
bash scripts/configure_ikc_reporting.sh --enforce --dry-run

# Patch only — skip pod restarts (pods already running)
bash scripts/configure_ikc_reporting.sh --enforce --skip-restart

# Revert to IBM defaults (both false)
bash scripts/configure_ikc_reporting.sh --disable
```

| Flag | Effect |
|---|---|
| `--enforce` | `enforceAuthorizeReporting=true` + `defaultAuthorizeReporting=true` (locked on) |
| `--disable` | Both flags → `false` (IBM factory default) |
| `--skip-restart` | Patch configmap + wkc-cr only; skip pod restarts and deletions |
| `--namespace NS` | CPD operands namespace (default: `cpd-instance`) |
| `--dry-run` | Preview only — nothing is changed |

### 11 · `reset_demo.sh` — full reset for a 100% clean rerun

One command that resets any combination of the three demo "surfaces":

```bash
scripts/reset_demo.sh --all --dry-run     # preview a full wipe (changes nothing)
scripts/reset_demo.sh --all               # Docker + schemas + MinIO
scripts/reset_demo.sh --docker            # just the local containers/volumes/images
scripts/reset_demo.sh --warehouse -y      # drop schemas + MinIO files, skip the prompt
```

| Flag | What it resets |
|---|---|
| `--docker` | Stops & removes the Metabase, Airflow, OpenMetadata stacks — containers + named volumes + the demo's **own (locally built)** images. Shared public base images are kept by default. |
| `--schemas` | Drops the watsonx.data schemas + tables/views (calls `cleanup_watsonxdata.py`). |
| `--minio` | Deletes the demo's MinIO/S3 files (calls `cleanup_minio.py`; needs `oc`). |
| `--warehouse` | `--schemas` then `--minio` (the whole remote side). |
| `--all` | `--docker` + `--schemas` + `--minio`. |
| `--dry-run` | Show what would happen, change nothing. |
| `--keep-images` | With `--docker`, remove **no** images (only containers + volumes). |
| `--purge-base-images` | With `--docker`, **also** remove shared public base images (`postgres:16`, `metabase/metabase`, `elasticsearch`, `python:3.12-slim`). Off by default. |
| `-y`, `--yes` | Skip the confirmation prompt. |

!!! success "Scoped to this demo only — verified"
    Every deletion is bounded to this demo:

    * **Docker** — `down` is run per compose file; the leftover-volume sweep is filtered by
      Docker's own `com.docker.compose.project` label, so it can only ever match this demo's
      two projects (an unrelated stack such as `insurance_*` is never touched). Images: only
      the demo's locally built images are removed unless you pass `--purge-base-images`.
    * **Schemas** — only the exact schema names derived from your `.env`
      (`dbt_demo_*`, `spark_demo_*`, the cpdctl raw schema) are dropped.
    * **MinIO** — only the demo's own trailing-slash-scoped prefixes inside `iceberg-bucket`;
      the bucket is never emptied.

!!! danger "Still destructive — always `--dry-run` first"
    `reset_demo.sh` permanently removes the selected resources. Run `--dry-run` first to see
    the exact containers, volumes, schemas, and object counts that will go.
