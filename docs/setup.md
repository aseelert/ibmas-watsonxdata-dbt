# Workshop Setup

!!! abstract "What this step does"
    This page installs the tools, creates the Python environment, and connects your machine to watsonx.data. When this step is done your laptop can run dbt models, submit Spark jobs, and use the cpdctl CLI.

Run every command from the repository root unless a step says otherwise. The whole process takes about 15 minutes on a typical laptop with a decent internet connection.

---

## Prerequisites checklist

Before you start, confirm that each item below is in place. The "How to verify" column shows the exact command to run.

| Requirement | Why you need it | How to verify | Expected output |
|---|---|---|---|
| Python 3.11 | The virtual environment and all scripts require exactly 3.11 | `python3.11 --version` | `Python 3.11.x` |
| Git | Cloning the repo | `git --version` | Any version is fine |
| Docker Desktop | Only needed for the OpenMetadata lineage demo | `docker --version` | Any version is fine |
| watsonx.data connection JSON | Contains Presto host, instance ID, and SSL certificate | `ls watsonx_data/instance_details.json` | File must exist |
| watsonx.data Software Hub API key | Your personal authentication credential | Provided by your administrator | Keep this secret |

!!! warning "Python version matters"
    The `dbt-watsonx-presto` adapter has been tested against Python 3.11 only. Using 3.12 or 3.10 may produce dependency conflicts. If `python3.11 --version` fails, install Python 3.11 from [python.org](https://www.python.org/downloads/) before continuing.

---

## Step 1: Clone and enter the repo

A Git repository holds all the dbt models, scripts, and configuration files for this workshop. Clone it once and work from that directory for every subsequent step.

```bash
git clone https://github.com/aseelert/ibmas-watsonxdata-dbt.git
cd ibmas-watsonxdata-dbt
```

Expected output:

```text
Cloning into 'ibmas-watsonxdata-dbt'...
remote: Enumerating objects: ...
Resolving deltas: 100% (...)
```

!!! tip "Already cloned?"
    If you already have the repo, just `cd` into it and run `git pull` to make sure you have the latest version.

---

## Step 2: Create Python 3.11 virtual environment

A virtual environment is an isolated Python installation that keeps this workshop's packages separate from anything else on your laptop. This prevents version conflicts with other Python projects.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Your prompt will change to show `(.venv)` when the environment is active. You must activate it again (`source .venv/bin/activate`) whenever you open a new terminal.

The `requirements.txt` installs these packages:

| Package | Version pinned | Plain-English role |
|---|---|---|
| `dbt-core` | >=1.8,<2.0 | The dbt engine that compiles and runs SQL models |
| `dbt-watsonx-presto` | 0.1.2 | Adapter that translates dbt calls into Presto-compatible SQL |
| `presto-python-client` | 0.8.4 | Low-level Python driver for talking to the Presto endpoint |
| `boto3` / `requests` | >=1.34 / >=2.31 | S3-compatible client for MinIO uploads and REST calls |
| `mkdocs` + `mkdocs-material` | >=1.6 / >=9.5 | Builds this documentation site locally |

!!! note "pip install takes a minute"
    The total download is around 80 MB. This is normal — dbt pulls in a lot of SQL parsing libraries.

---

## Step 3: Create local `.env`

The `.env` file holds environment-specific values such as hostnames, usernames, and your API key. Keeping them in a file rather than hard-coding them means you never accidentally commit secrets to Git.

```bash
cp .env.example .env
```

Open `.env` in any text editor and set your Software Hub API key on this line:

```bash
WXD_API_KEY=<your-software-hub-api-key>
```

The key looks like a long alphanumeric string provided by your workshop administrator. Every other value in `.env` will be filled in automatically by Step 5.

!!! warning "Never commit .env"
    `.env` is already listed in `.gitignore`, so Git will not track it. Still, double-check by running `git status` — `.env` should never appear as a file to commit. Real API keys belong only in `.env` or your shell environment, never in source code.

---

## Step 4: Add the watsonx.data connection JSON

The connection JSON is a file you export from the watsonx.data console. It contains the Presto endpoint address, your instance ID, and the SSL certificate chain needed to establish a trusted TLS connection. It does **not** contain your API key.

To get the file:

1. Open the watsonx.data console in your browser.
2. Go to **Infrastructure manager** and click your Presto engine (`presto651`).
3. Click **Download connection details** (or the equivalent export button your administrator points you to).
4. Save the downloaded file as `watsonx_data/instance_details.json` inside the repo root.

The JSON contains fields similar to these:

```text
Field                   Plain-English meaning
─────────────────────── ──────────────────────────────────────────────────
Presto host / port      Where dbt sends SQL queries
                        (ibm-lh-lakehouse-presto651-presto-svc.apps...
                        :443)
instance_id             Identifies your watsonx.data tenant on the cluster
cpd_host                The IBM Software Hub (CPD) base URL
ssl_certificate         PEM-encoded CA certificate chain for TLS
```

!!! info "One JSON per environment"
    The administrator may give you a pre-exported JSON for the workshop cluster. If so, just copy it to `watsonx_data/instance_details.json` and skip the export steps above.

---

## Step 5: Import connection values

The `prepare_watsonx_env.py` script reads `watsonx_data/instance_details.json`, extracts the non-secret values, writes the SSL certificate to disk, and updates your `.env` file automatically. You only need to run this once (or again if the JSON changes).

```bash
python scripts/prepare_watsonx_env.py
```

Expected output:

```text
Read connection details from: watsonx_data/instance_details.json
Wrote certificate chain to: certs/watsonxdata-ca.pem
Updated env file: .env

Imported values:
  WXD_INSTANCE_ID=<your-instance-id>
  WXD_HOST=ibm-lh-lakehouse-presto651-presto-svc.apps.watson.ibmas-zocp-techcluster.org
  WXD_PORT=443
  WXD_PRESTO_ENGINE_ID=presto651
  WXD_CPD_HOST=cpd-cpd-instance.apps.watson.ibmas-zocp-techcluster.org
  WXD_CPD_AUTH_URL=https://cpd-cpd-instance.apps.watson.ibmas-zocp-techcluster.org/icp4d-api/v1/authorize
  WXD_SSL_VERIFY=certs/watsonxdata-ca.pem
  WXD_CATALOG=iceberg_data
  WXD_SCHEMA=lakehouse_demo
```

After this step two things will have changed on disk:

- `certs/watsonxdata-ca.pem` — the CA certificate that both dbt and cpdctl use to verify the cluster's TLS certificate.
- `.env` — now contains the Presto host, port, instance ID, and CPD host in addition to your API key.

!!! tip "JSON file in a different location?"
    If your exported JSON lives somewhere else on your laptop, pass the path explicitly:

    ```bash
    python scripts/prepare_watsonx_env.py --connection-json /path/to/presto-connection.json
    ```

    If the JSON changed (for example, the administrator rotated the certificate) and you want to overwrite the values already in `.env`, add the `--overwrite` flag:

    ```bash
    python scripts/prepare_watsonx_env.py --overwrite
    ```

---

## Step 6: Create the dbt profile

dbt reads connection details from a profiles file in your home directory. The repo ships an example profile that uses the environment variables you set in Steps 3 and 5.

```bash
mkdir -p ~/.dbt
cp profiles/profiles.example.yml ~/.dbt/profiles.yml
```

The profile connects dbt to Presto using `BasicAuth`. It picks up every value — host, port, catalog, schema, SSL cert path — from the environment variables already in your `.env`, so you do not need to edit the file manually.

!!! note "Why ~/.dbt and not the project folder?"
    dbt looks for `profiles.yml` in `~/.dbt` by default. Keeping it there means your API key is never inside the project directory where you might accidentally commit it.

---

## Step 7: Verify the connection (recommended)

Before running any dbt models, confirm that your laptop can actually reach the Presto engine. This catches typos in the API key, wrong certificate paths, or network firewall issues before they interrupt the main workshop flow.

Run either of these two checks (they test the same connection from different angles):

```bash
# Option A: run a small gold-layer query via Python
python scripts/query_gold.py
```

```bash
# Option B: ask dbt to verify its own profile
bash scripts/dbt_env.sh debug
```

Successful output for Option A shows the gold mart tables (run it after completing the full pipeline in [Path A — dbt](dbt-demo.md)):

```text
Daily Sales
===========
+------------+-------------+-------------+------------+-------------+
| ORDER_DATE | CATEGORY    | ORDER_COUNT | UNITS_SOLD | NET_REVENUE |
+------------+-------------+-------------+------------+-------------+
| 2026-01-03 | Electronics |           1 |          4 |      392.00 |
| 2026-01-03 | Home        |           2 |          6 |      182.40 |
...
494 rows
```

Successful output for Option B ends with:

```text
Connection:
  host: ibm-lh-lakehouse-presto651-presto-svc.apps.watson.ibmas-zocp-techcluster.org
  port: 443
  user: ibmlhapikey_cpadmin
  database: iceberg_data
  schema: lakehouse_demo
  ssl_verify: certs/watsonxdata-ca.pem
Registered adapter: watsonx_presto=0.1.2
  Connection test: [OK connection ok]

All checks passed!
```

!!! warning "Connection refused or SSL error?"
    See the [Troubleshooting](troubleshooting.md) page. The most common causes are a missing `certs/watsonxdata-ca.pem` (re-run Step 5) or a wrong API key (check `.env`).

---

## Step 8: Install cpdctl (Path C only)

!!! info "Skip this step unless you plan to run Path C"
    Path C uses the IBM `cpdctl` CLI to submit native ingestion jobs that appear in the watsonx.data console under **Data manager → Ingestion**. If you are only running the dbt path or the Spark path, skip to the "Ready to go?" section below.

`cpdctl` is the IBM Cloud Pak for Data command-line interface. It talks to the watsonx.data ingestion service directly, bypassing dbt/Spark for the LOAD step (you still use dbt or Spark to transform the loaded data). cpdctl only INGESTS raw CSV into lakehouse_demo_ingest — it is a loader, not a transformation engine. To build a medallion on top you run the dbt or Spark transforms against the ingest schema afterward.

Install on macOS (Apple Silicon):

```bash
curl -fsSL -o cpdctl.tar.gz \
  https://github.com/IBM/cpdctl/releases/download/v1.8.233/cpdctl_darwin_arm64.tar.gz
tar -xzf cpdctl.tar.gz -C ~/.local/bin && chmod +x ~/.local/bin/cpdctl
cpdctl version
```

Expected output:

```text
cpdctl version 1.8.233 ...
```

!!! tip "Other operating systems"
    Find the correct binary for your OS at [github.com/IBM/cpdctl/releases](https://github.com/IBM/cpdctl/releases). Replace `cpdctl_darwin_arm64.tar.gz` with the filename for your platform (for example, `cpdctl_linux_amd64.tar.gz` for Linux x86-64).

After installing the binary, configure a profile for this workshop environment. The values come from `.env`:

```bash
set -a; source .env; set +a

cpdctl config profile set wxd-demo \
  --url "https://${WXD_CPD_HOST}" \
  --username "${WXD_CPD_USERNAME}" \
  --apikey "${WXD_API_KEY}" \
  --env "WATSONX_DATA_INSTANCE_ID=${WXD_INSTANCE_ID}"
```

Then tell cpdctl to trust the cluster CA certificate:

```bash
export SSL_CERT_FILE="$PWD/certs/watsonxdata-ca.pem"
```

!!! note "Add SSL_CERT_FILE to your shell profile"
    If you close and reopen the terminal, you will need to re-export `SSL_CERT_FILE`. Consider adding it to `~/.zshrc` or `~/.bashrc` so it persists across sessions.

---

## Ready to go?

Before moving on, confirm all four items below:

- [ ] `(.venv)` appears in your terminal prompt (virtual environment is active).
- [ ] `python scripts/query_gold.py` or `bash scripts/dbt_env.sh debug` printed `OK` or returned a row count.
- [ ] `certs/watsonxdata-ca.pem` exists (run `ls certs/watsonxdata-ca.pem`).
- [ ] `.env` contains a real value for `WXD_API_KEY` (not the placeholder `replace-with-your-software-hub-api-key`).

If every item above is checked, your laptop is fully configured.

Next: [Architecture & Data Flow](lineage.md)
