# Fast Track — All Commands

!!! abstract "What this page is"
    Every command in the workshop, in order, with the *minimum* explanation — for people who have
    already read the full pages once and just want to copy-paste. Each block links back to the
    page that explains it. **First time through? Use the full pages instead** — start at
    [Prepare Your Machine](setup.md).

!!! warning "Run setup first"
    Everything below assumes you completed [Setup](setup.md): Python 3.11 `.venv`, `.env`
    populated, dbt profile in place, and `bash scripts/dbt_env.sh debug` passing. Commands are
    copy-paste for **macOS/Linux (zsh/bash)**.

---

## 0 · One-time setup → [setup.md](setup.md)

```bash
git clone https://github.com/aseelert/ibmas-watsonxdata-dbt.git
cd ibmas-watsonxdata-dbt
python3.11 -m venv .venv                 # Python 3.11 REQUIRED (3.14 breaks dbt)
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env                     # then edit .env — see Configuration reference
python scripts/prepare_watsonx_env.py    # parses instance JSON → .env + certs/watsonxdata-ca.pem
mkdir -p ~/.dbt
cp profiles/profiles.example.yml ~/.dbt/profiles.yml
python scripts/query_gold.py             # smoke-test the Presto connection
bash scripts/dbt_env.sh debug            # dbt sees the profile + connects
```

Optional CLI tools (only needed for Spark asset upload and cpdctl):

```bash
mkdir -p ~/.local/bin
export PATH="$HOME/.local/bin:$PATH"
# OpenShift CLI (oc)
curl -fsSL -o oc.tar.gz https://mirror.openshift.com/pub/openshift-v4/clients/ocp/latest/openshift-client-mac-arm64.tar.gz
tar -xzf oc.tar.gz -C ~/.local/bin oc && chmod +x ~/.local/bin/oc
oc version --client
python scripts/check_hosts.py            # verify /etc/hosts entries for the cluster routes
```

---

## A · dbt — full medallion → [dbt-demo.md](dbt-demo.md)

```bash
cd /Users/aseelert/GitHub/ibmas-watsonxdata-dbt   # your repo path
source .venv/bin/activate
python scripts/prepare_watsonx_env.py
python scripts/bootstrap_watsonxdata.py           # create dbt_demo_{raw,bronze,silver,gold}
bash scripts/dbt_env.sh seed --full-refresh       # load 4 CSVs → dbt_demo_raw
bash scripts/dbt_env.sh run                        # build bronze → silver → gold
bash scripts/dbt_env.sh test                       # schema + data tests
python scripts/query_gold.py                        # query the gold marts
```

One-command alternative and per-layer selection:

```bash
bash scripts/dbt_env.sh build --full-refresh        # seed + run + test in one go
bash scripts/dbt_env.sh run --select tag:bronze
bash scripts/dbt_env.sh run --select tag:silver
bash scripts/dbt_env.sh run --select tag:gold
```

---

## B · Spark — full medallion → [spark-demo.md](spark-demo.md)

```bash
cd /Users/aseelert/GitHub/ibmas-watsonxdata-dbt
source .venv/bin/activate
python scripts/upload_spark_assets.py               # push job + CSVs to MinIO
WXD_SPARK_DRY_RUN=true  python scripts/submit_spark_application.py   # preview
WXD_SPARK_DRY_RUN=false python scripts/submit_spark_application.py   # submit for real
python scripts/spark_application_status.py <application-id>          # poll until finished
```

---

## C · cpdctl — native raw ingest → [ingestion.md](ingestion.md)

```bash
set -a; source .env; set +a
export SSL_CERT_FILE="$PWD/certs/watsonxdata-ca.pem"
python scripts/upload_spark_assets.py               # stage CSVs in object storage
python scripts/ingest_with_cpdctl.py                # kick off the ingestion jobs
python scripts/ingest_with_cpdctl.py --wait         # or wait for completion
```

cpdctl lands **raw only** in `spark_demo_cpdctl_raw`. Build a medallion on top with dbt or Spark
afterward (see [When to Use Which](choosing.md)).

---

## Confluent — streaming (Kafka → Flink → Iceberg) → [confluent-demo.md](confluent-demo.md)

```bash
bash confluent/scripts/expose_minio_route.sh        # once: OpenShift Route so Docker reaches MinIO
python scripts/check_hosts.py
bash confluent/start.sh --all                        # build Flink image, start services, seed topics
bash confluent/start.sh --silver                     # run the Flink silver pipeline
python confluent/scripts/prep_iceberg_schemas.py --phase register   # register silver tables in catalog
python confluent/scripts/submit_confluent_gold.py --no-dry-run --wait   # build gold (Spark engine)
python scripts/reconcile_gold.py                     # prove dbt = Spark = Confluent gold
```

Stop / reset the streaming stack:

```bash
bash confluent/start.sh --stop
bash confluent/start.sh --reset -y
```

---

## D · DataStage — no-code Confluent gold → [datastage-demo.md](datastage-demo.md)

```bash
python scripts/get_token.py                          # sanity-check auth
python confluent/scripts/create_datastage_flow.py    # DRY RUN (default) — preview the request
python confluent/scripts/create_datastage_flow.py --apply        # create the flow on the cluster
python confluent/scripts/create_datastage_flow.py --apply --run  # create + compile + run
python scripts/reconcile_gold.py                     # confirm DataStage gold matches the rest
```

!!! note "Needs a live DataStage service"
    `create_datastage_flow.py` requires a CP4D/Software Hub cluster with the DataStage cartridge.
    It defaults to dry-run; nothing is sent until you pass `--apply`.

---

## Orchestrate — Airflow → [airflow.md](airflow.md)

```bash
cp .env.example .env                                  # if not already done
cp profiles/profiles.example.yml profiles/profiles.yml
docker compose build airflow-webserver airflow-scheduler airflow-dag-processor airflow-init
docker compose up airflow-init
docker compose up -d
open http://localhost:8082                            # admin / admin
```

Stop / reset:

```bash
docker compose down            # stop
docker compose down -v         # stop + wipe volumes
```

---

## Compare & reconcile → [sql-demo.md](sql-demo.md)

```bash
python scripts/query_gold.py                          # dbt gold marts
python scripts/reconcile_gold.py                      # 3-way parity: dbt vs Spark vs Confluent
```

Run the side-by-side SQL in the watsonx.data SQL editor — see [the SQL page](sql-demo.md).

---

## Reset the whole demo → [troubleshooting.md](troubleshooting.md)

```bash
bash scripts/reset_demo.sh --warehouse               # drop demo schemas / warehouse data
python scripts/cleanup_watsonxdata.py                # drop watsonx.data demo schemas
python scripts/cleanup_minio.py                      # clear MinIO demo objects
```

---

## See also

- Every `.env` variable explained: [Configuration & .env Reference](configuration.md).
- What each script does and its flags: [Scripts](scripts.md).
- Where each file lives: [File Guide](files.md).
