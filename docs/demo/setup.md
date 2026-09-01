# Baseline workshop

The baseline is the shortest complete story: source fixtures become tested
Iceberg Gold marts and a business dashboard. This page contains every command
you need, from a fresh clone to a running dashboard.

---

## Step 1 — Python 3.11

The repo pins Python to **3.11** (see `.python-version`). Install it if you
don't have it, then create the virtual environment:

=== "Homebrew (macOS)"

    ```bash
    brew install python@3.11
    python3.11 --version          # Python 3.11.x
    ```

=== "uv (any OS)"

    ```bash
    # Install uv once:
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

=== "pyenv"

    ```bash
    pyenv install 3.11
    pyenv local 3.11
    ```

### Create and activate the virtual environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate         # macOS / Linux
# .venv\Scripts\activate.bat      # Windows
```

### Install dependencies

```bash
pip install -r requirements.txt
```

Verify dbt is available:

```bash
dbt --version
```

---

## Step 2 — Docker runtime

The optional UI services (Metabase, Airflow, Marquez, OpenMetadata) run in
Docker. `dbt` and `spark` talk directly to the remote watsonx.data cluster —
**they do not need Docker**.

=== "OrbStack (macOS — recommended)"

    ```bash
    open -a OrbStack              # start OrbStack; it auto-starts on login
    # or via CLI:
    orbctl start
    ```

=== "Docker Desktop"

    Start Docker Desktop from the Applications folder or menu bar.

=== "Linux (Docker Engine)"

    ```bash
    sudo systemctl start docker
    ```

Confirm Docker Compose v2 is available:

```bash
docker compose version            # must show v2.x, not docker-compose 1.x
```

---

## Step 3 — Build custom Docker images

Two services need custom images built locally before they can start:

| Image | Used by | Build time |
| --- | --- | --- |
| Airflow (`05-airflow/airflow/Dockerfile`) | `bin/demo docker start airflow` | ~1–2 min |
| Flink `wxd-flink:1.20` (`04-confluent-streaming/confluent/flink/Dockerfile`) | `bin/demo streaming` | ~3–5 min (downloads ~200 MB of JARs) |

Build both at once:

```bash
bin/demo docker build
```

You only need to do this **once after cloning**. Re-run it after any
`git pull` that changes a `Dockerfile` or `requirements.txt`.

---

## Step 4 — Configure `.env`

```bash
cp .env.example .env
```

The `.env` file holds credentials and connection endpoints. The recommended
path is automatic discovery:

1. Export the Presto connection JSON from the watsonx.data UI
2. Save it as `watsonx_data/instance_details.json`
3. Set `WXD_OC_PASSWORD` in `.env` (the one value the script cannot discover)
4. Run:

```bash
bin/demo setup
```

`bin/demo setup` logs into OpenShift, derives all ~40 connection values, writes
`certs/watsonxdata-ca.pem`, and fetches the bearer token. Run it again at the
start of every session — tokens expire in ~12 hours.

Do not commit `.env`, certificates, tokens, or the connection JSON.

---

## Step 5 — Manage Docker services

### Start

```bash
# Start a single service:
bin/demo docker start metabase    # BI dashboard      → http://localhost:3000
bin/demo docker start airflow     # DAG scheduler     → http://localhost:8082
bin/demo docker start lineage     # Marquez lineage   → http://localhost:3001
bin/demo docker start catalog     # OpenMetadata      → http://localhost:8585

# Start ALL services at once (≥ 16 GB RAM recommended):
bin/demo docker start
```

### Status

```bash
bin/demo docker status
```

Shows running/stopped state for all demo containers across all stacks.

### Stop

```bash
# Stop a single service:
bin/demo docker stop metabase
bin/demo docker stop airflow
bin/demo docker stop lineage
bin/demo docker stop catalog

# Stop ALL services:
bin/demo docker stop
```

!!! note "dbt and Spark do not need Docker"
    `bin/demo dbt build` and `bin/demo spark` connect directly to the remote
    watsonx.data Presto and Spark engines. Start Docker services only for the
    optional BI, orchestration, lineage, and catalog UIs.

---

## Step 6 — Run and validate

```bash
# Verify the watsonx.data connection:
bin/demo dbt debug

# Build the full medallion (Bronze → Silver → Gold):
bin/demo dbt build

# Start Metabase to view the Gold dashboard:
bin/demo docker start metabase
```

Open [http://localhost:3000](http://localhost:3000) — login with the credentials
configured in `.env` (default: `admin@admin.com` / `admin12345`).

```bash
# Compare Gold output across all paths (dbt / Spark / Confluent):
bin/demo validate
```

The Spark and Kafka/Flink paths can now be demonstrated as independent
implementations of the same Gold contract.

---

## Every-session checklist

```bash
source .venv/bin/activate         # re-activate venv
bin/demo setup                    # refresh credentials (tokens expire ~12 h)
bin/demo docker status            # check which services are already running
```

---

## Teardown

```bash
# Stop all Docker services:
bin/demo docker stop

# Preview what a full reset would remove:
bin/demo reset --dry-run

# Execute the reset:
bin/demo reset --docker
```

See [Scripts and automation](scripts.md) for the full reset reference and
[Docker services](docker-services.md) for per-stack `docker compose` commands.
