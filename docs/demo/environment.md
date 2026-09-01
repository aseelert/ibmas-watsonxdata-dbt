# Environment setup

Everything the demo runs on your laptop needs three foundations in place before
any `bin/demo` command works: **Python 3.11**, a **Docker-compatible runtime**,
and a populated **`.env`** file. This page covers each one in order.

---

## 1 — Python 3.11

The repo pins Python to **3.11** (see [`.python-version`](../../.python-version)).
`dbt-watsonx-presto` and several other dependencies are not yet tested against
3.12+, so using the pinned version avoids surprises.

=== "Homebrew (macOS)"

    ```bash
    brew install python@3.11
    # Confirm:
    python3.11 --version          # Python 3.11.x
    ```

=== "uv (any OS)"

    [`uv`](https://github.com/astral-sh/uv) can manage Python versions without
    Homebrew or pyenv:

    ```bash
    # Install uv (once):
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # uv reads .python-version and installs 3.11 automatically:
    uv venv .venv
    source .venv/bin/activate
    uv pip install -r requirements.txt
    ```

=== "pyenv"

    ```bash
    pyenv install 3.11
    pyenv local 3.11              # writes .python-version (already present)
    python --version              # Python 3.11.x
    ```

### Virtual environment

Create and activate the virtual environment once. Re-activate it at the start
of every session.

```bash
python3.11 -m venv .venv
source .venv/bin/activate         # macOS / Linux
# .venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

Keep `.venv/` out of Git — it is already in [`.gitignore`](../../.gitignore).

---

## 2 — Docker runtime

The optional local services (Metabase, Airflow, Confluent/Flink, Marquez,
OpenMetadata) all run as Docker Compose stacks. You need a Docker-compatible
runtime that provides `docker compose` (v2).

=== "OrbStack (recommended on macOS)"

    [OrbStack](https://orbstack.dev) is a lightweight Docker Desktop alternative
    for macOS. It starts faster, uses less RAM, and is already installed on this
    machine.

    ```bash
    # Start OrbStack (if not already running):
    open -a OrbStack
    # or via the CLI:
    orbctl start

    # Confirm Docker is reachable:
    docker info --format '{{.ServerVersion}}'
    docker compose version
    ```

    OrbStack auto-starts at login by default. If `docker` commands fail, check
    that the OrbStack menu-bar icon shows **Running**.

=== "Docker Desktop (macOS / Windows / Linux)"

    Download from [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/).
    After installation, start Docker Desktop and confirm:

    ```bash
    docker info --format '{{.ServerVersion}}'
    docker compose version
    ```

=== "Docker Engine (Linux)"

    ```bash
    # Debian / Ubuntu:
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    sudo usermod -aG docker $USER   # re-login after this

    docker info --format '{{.ServerVersion}}'
    docker compose version
    ```

### Build the custom images (one-time)

Two services require custom Docker images. Build both with:

```bash
bin/demo docker build
```

This runs in ~3–5 minutes on first use (downloads ~200 MB of Flink JARs).
Re-run after any `git pull` that touches a `Dockerfile` or `requirements.txt`.

!!! note "dbt and Spark do NOT need Docker"
    `bin/demo dbt build` and `bin/demo spark` talk directly to the remote
    watsonx.data Presto/Spark engines. Only the optional UI services
    (Metabase, Airflow, Marquez, OpenMetadata) run in Docker.

### Start and stop services

```bash
# Start a single service:
bin/demo docker start metabase     # BI dashboard    → http://localhost:3000
bin/demo docker start airflow      # DAG scheduler   → http://localhost:8082
bin/demo docker start lineage      # Marquez lineage → http://localhost:3001
bin/demo docker start catalog      # OpenMetadata    → http://localhost:8585

# Start all services at once (heavy — ≥ 16 GB RAM recommended):
bin/demo docker start

# Check what is running:
bin/demo docker status

# Stop individual services or all at once:
bin/demo docker stop metabase
bin/demo docker stop
```

See [Docker services](docker-services.md) for the full list of what each image
contains, port assignments, and direct `docker compose` commands.

---

## 3 — Watsonx.data connection and `.env`

```bash
cp .env.example .env
```

The `.env` file holds credentials and connection endpoints. There are two paths
to fill it in:

**Automatic (recommended):**  
Export the Presto connection JSON from the watsonx.data UI and place it at
`watsonx_data/instance_details.json`, then run:

```bash
bin/demo setup
```

`bin/demo setup` logs into OpenShift, derives all ~40 URLs and config values
from the JSON, writes the CA certificate to `certs/watsonxdata-ca.pem`, and
fetches the API key and Spark bearer token. It is safe and cheap to re-run at
the start of every session — credentials expire, so run it first.

**Manual:**  
Edit `.env` directly. The required fields are marked `# SECRET` or `# MANUAL`
in `.env.example`; everything marked `# AUTO` is written by `bin/demo setup`.

Do not commit `.env`, tokens, API keys, certificates, or connection JSON files.

---

## Full first-time sequence

```bash
# ── 1. Python ─────────────────────────────────────────────────────────────────
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# ── 2. Docker runtime ─────────────────────────────────────────────────────────
open -a OrbStack                  # start OrbStack (macOS); skip if already running
docker compose version            # must print 'Docker Compose version v2.x'

# ── 3. Build custom images (once) ─────────────────────────────────────────────
bin/demo docker build             # Airflow + Flink images (~3–5 min first time)

# ── 4. Connect to watsonx.data ────────────────────────────────────────────────
cp .env.example .env              # fill in WXD_OC_PASSWORD and paste the JSON
bin/demo setup                    # login, derive endpoints, write cert + tokens

# ── 5. Run dbt (no Docker required) ──────────────────────────────────────────
bin/demo dbt build

# ── 6. Start UI services and validate ────────────────────────────────────────
bin/demo docker start metabase    # Metabase dashboard → http://localhost:3000
bin/demo validate                 # compare Gold across all paths
```

At the start of every subsequent session, re-activate the venv and re-run
setup (tokens are short-lived):

```bash
source .venv/bin/activate
bin/demo setup
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `command not found: docker` | Docker runtime not running | Start OrbStack or Docker Desktop |
| `docker compose: command not found` | Compose v1 (`docker-compose`) installed instead of v2 | Install Docker Compose plugin or use OrbStack/Docker Desktop |
| `dbt-watsonx-presto` install fails | Wrong Python version | Use Python 3.11 (see above) |
| `Error: image wxd-flink:1.20 not found` | Flink image not built | Run `bin/demo docker build` |
| `Connection refused` on Presto | Token or cert expired | Re-run `bin/demo setup` |
| `bin/demo dbt debug` fails with TLS error | CA cert not written | Re-run `bin/demo setup` with the connection JSON in place |

Before a destructive reset, inspect the scope:

```bash
bin/demo reset --dry-run
```

See [Access and interfaces](access.md) for all local service ports and
[Docker services](docker-services.md) for detailed per-stack startup commands.
