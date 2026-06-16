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
| OpenShift CLI (`oc`) | **Spark + cpdctl paths only:** reads the MinIO secret and opens the port-forward to object storage. Not needed for the dbt path. Install in [Step 8](#step-8-install-command-line-tools-oc-cpdctl). | `oc version --client` | `Client Version: 4.x.x` |
| IBM `cpdctl` | **cpdctl path only:** submits native ingestion jobs. Install in [Step 8](#step-8-install-command-line-tools-oc-cpdctl). | `cpdctl version` | `cpdctl version 1.x.x ...` |
| Docker Desktop | Only needed for the OpenMetadata lineage demo | `docker --version` | Any version is fine |
| watsonx.data connection JSON | Contains Presto host, instance ID, and SSL certificate | `ls watsonx_data/instance_details.json` | File must exist |
| watsonx.data Software Hub API key | Your personal authentication credential | Provided by your administrator | Keep this secret |

!!! info "Which tools does each path need?"
    - **dbt path** — Python 3.11 + the `.venv` packages only. No `oc`, no `cpdctl`.
    - **Spark path** — also needs **`oc`** (to read the MinIO credentials secret and port-forward to object storage when uploading the PySpark app and CSVs).
    - **cpdctl path** — also needs **`oc`** and **`cpdctl`**.
    - **OpenMetadata** — also needs **Docker**.

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

=== "macOS / Linux"

    ```bash
    python3.11 -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    ```

=== "Windows (PowerShell)"

    ```powershell
    py -3.11 -m venv .venv
    .venv\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    pip install -r requirements.txt
    ```

    If `Activate.ps1` is blocked, run PowerShell as your user and allow local scripts once:
    `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

Your prompt will change to show `(.venv)` when the environment is active. You must activate it again (re-run the activate command) whenever you open a new terminal.

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

### What is in `.env` and why

`.env` is the single place that holds your secret and your environment-specific values. The dbt
profile and every script read from it, so you edit *one* file to point the whole workshop at your
cluster. You provide just the **API key** by hand; `prepare_watsonx_env.py` (Step 5) fills in the
rest from the connection JSON. Values below marked **auto** are written for you; **manual** values
already have a working default in the template.

??? info "Core Presto / dbt connection — what each line means"

    ```bash
    WXD_CPD_USERNAME=cpadmin                              # your Software Hub login name (manual)
    WXD_USER=ibmlhapikey_cpadmin                          # Presto user = ibmlhapikey_<username> (manual)
    WXD_API_KEY=replace-with-your-...                     # SECRET you paste in — NOT in the JSON
    WXD_CATALOG=iceberg_data                              # Iceberg catalog (auto, default)
    WXD_SCHEMA=lakehouse_demo                             # base schema prefix (auto, default)
    WXD_HOST=...presto651-presto-svc...                   # Presto endpoint (auto)
    WXD_PORT=443                                          # Presto port (auto)
    WXD_INSTANCE_ID=1781163689818519                      # tenant ID -> LhInstanceId header (auto)
    WXD_PRESTO_ENGINE_ID=presto651                        # which Presto engine (auto)
    WXD_SSL_VERIFY=certs/watsonxdata-ca.pem               # path to the CA cert (auto, written for you)
    WXD_GOLD_MATERIALIZED=view                            # build gold as views vs tables (manual)
    ```

    **Why:** these are exactly the values the dbt profile reads (Step 6). `WXD_API_KEY` is the one
    true secret you supply — it is never stored in the connection JSON, which is why you paste it here.

??? info "Software Hub / OpenShift endpoints — Spark & cpdctl paths"

    ```bash
    WXD_CPD_HOST=cpd-cpd-instance.apps...                 # Software Hub base host (auto)
    WXD_CPD_AUTH_URL=https://.../icp4d-api/v1/authorize   # token endpoint (auto)
    WXD_OPENSHIFT_CONSOLE=https://console-openshift...    # OpenShift web console (reference)
    WXD_OPENSHIFT_API=https://api.watson...:6443          # used for `oc login`
    ```

    **Why:** the Spark and cpdctl paths authenticate against Software Hub and talk to the OpenShift
    cluster (to read the MinIO secret and open the port-forward).

??? info "Spark path + MinIO object store"

    ```bash
    WXD_SPARK_SCHEMA=spark_demo                           # Spark writes to spark_demo_* (own schemas)
    WXD_SPARK_ENGINE_ID=spark656                          # the Spark engine to submit to
    WXD_SPARK_APPLICATION=s3a://.../load_medallion_demo.py # uploaded PySpark app
    WXD_SPARK_INPUT_BASE=s3a://.../raw                    # uploaded CSVs
    WXD_SPARK_DRY_RUN=true                                # print the job payload without submitting
    WXD_OBJECT_STORE_ENDPOINT=http://127.0.0.1:19000      # localhost:port served by the oc port-forward
    # WXD_OBJECT_STORE_ACCESS_KEY / _SECRET_KEY           # if unset, read from the oc secret (still needs the port-forward)
    WXD_OBJECT_STORE_SECRET_NAME=ibm-lh-minio-secret      # OpenShift secret holding the MinIO keys
    ```

    **Why:** Spark runs its own medallion into `spark_demo_*` so you can compare it with dbt. The app
    and CSVs must live in MinIO before the engine can read them — if you do not set the MinIO keys,
    the uploader reads them from the `ibm-lh-minio-secret` secret (this is why the Spark path needs
    `oc`).

    !!! warning "Why `oc` can't simply be skipped here"
        MinIO (`ibm-lh-lakehouse-minio-svc`) is a **ClusterIP** service with **no Route**, so it is
        not reachable from your laptop directly — the `oc` port-forward is the only way in. Supplying
        the access/secret keys manually only skips *reading the secret*, not the tunnel. Going fully
        `oc`-free would require an administrator to expose MinIO via an OpenShift Route (a security
        decision). The actual upload is pure Python (boto3) either way.

The complete per-variable table (including the optional OpenMetadata variables) is in the
[Configuration & Files Reference](configuration.md#env-reference-every-variable).

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

=== "macOS / Linux"

    ```bash
    mkdir -p ~/.dbt
    cp profiles/profiles.example.yml ~/.dbt/profiles.yml
    ```

=== "Windows (PowerShell)"

    ```powershell
    New-Item -ItemType Directory -Force "$env:USERPROFILE\.dbt" | Out-Null
    Copy-Item profiles\profiles.example.yml "$env:USERPROFILE\.dbt\profiles.yml"
    ```

The profile connects dbt to Presto using `BasicAuth`. It picks up every value — host, port, catalog, schema, SSL cert path — from the environment variables already in your `.env`, so you do not need to edit the file manually.

### What the profile contains and why

```yaml
watsonxdata_medallion_demo:        # profile name — matches `profile:` in dbt_project.yml
  target: dev                      # which output block to use by default
  outputs:
    dev:
      type: watsonx_presto         # the dbt-watsonx-presto adapter — speaks Presto
      method: BasicAuth            # auth: username + API key over HTTPS
      user: "{{ env_var('WXD_USER', 'ibmlhapikey_cpadmin') }}"
      password: "{{ env_var('WXD_API_KEY') }}"
      catalog: "{{ env_var('WXD_CATALOG', 'iceberg_data') }}"
      schema: "{{ env_var('WXD_SCHEMA', 'lakehouse_demo') }}"
      host: "{{ env_var('WXD_HOST', '...presto651-presto-svc...') }}"
      port: "{{ env_var('WXD_PORT', '443') | int }}"
      ssl_verify: "{{ env_var('WXD_SSL_VERIFY', 'certs/watsonxdata-ca.pem') }}"
      http_headers:
        LhInstanceId: "{{ env_var('WXD_INSTANCE_ID', '...') }}"
      threads: "{{ env_var('DBT_THREADS', '4') | int }}"
```

| Field | Meaning | Why it is needed |
|---|---|---|
| `type: watsonx_presto` | Loads the IBM Presto adapter | dbt has no built-in watsonx.data driver; this adapter turns your SQL models into Presto `CREATE TABLE AS` statements |
| `method: BasicAuth` | Username + API-key auth | watsonx.data accepts your API key as the password for an `ibmlhapikey_*` user |
| `user` / `password` | The Presto login | `password` is your **secret API key** — read from `.env`, never written in the file |
| `catalog` / `schema` | Default namespace | `iceberg_data.lakehouse_demo*` — where dbt writes bronze/silver/gold |
| `host` / `port` | Where to send SQL | The Presto engine endpoint (`:443`) |
| `ssl_verify` | Path to the CA cert | Presto uses TLS; dbt must trust the cluster cert or the connection is refused |
| `http_headers.LhInstanceId` | Your tenant ID | watsonx.data is multi-tenant — this header routes queries to **your** instance |
| `threads` | Parallel model builds | How many models compile/run at once |

**The key idea:** the profile holds **no literal secrets** — every `{{ env_var('...') }}` reads from
your `.env` at runtime (the `scripts/dbt_env.sh` wrapper loads `.env` before calling dbt). So you copy
this template once and never edit it; to point dbt at a different cluster you change only `.env`.

!!! note "Why ~/.dbt and not the project folder?"
    dbt looks for `profiles.yml` in `~/.dbt` by default. Keeping it there means your API key is never inside the project directory where you might accidentally commit it.

### How the connection JSON, `.env`, and the profile fit together

```text
watsonx_data/instance_details.json     ← you export from the watsonx.data console (Step 4)
            │   python scripts/prepare_watsonx_env.py   (Step 5)
            ▼
.env   ← host, port, instance ID, cert path auto-filled; you paste WXD_API_KEY (Step 3)
            │   scripts/dbt_env.sh loads .env before calling dbt
            ▼
~/.dbt/profiles.yml   ← {{ env_var('WXD_*') }} reads the values (Step 6)
            │
            ▼
dbt connects to Presto and builds the medallion
```

Secrets and cluster-specific values live in **`.env`** (git-ignored, one file to edit); the **dbt
profile** is a fixed template that pulls them in. Nothing sensitive is committed, and switching
environments means changing only `.env` — never the profile or the SQL.

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

## Step 8: Install command-line tools (`oc`, `cpdctl`)

!!! info "Skip this step if you are only running the dbt path"
    The dbt path needs nothing beyond the Python virtual environment. Install `oc` if you plan to
    run the **Spark path** (it reads the MinIO secret and port-forwards to object storage). Install
    **both** `oc` and `cpdctl` if you plan to run the **cpdctl path**.

First make sure your local bin directory exists and is on your `PATH`:

```bash
mkdir -p ~/.local/bin
echo "$PATH" | tr ':' '\n' | grep -q "$HOME/.local/bin" || \
  echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc   # use ~/.bashrc on Linux
export PATH="$HOME/.local/bin:$PATH"
```

### 8a · OpenShift CLI (`oc`) — needed for the Spark and cpdctl paths

`oc` is the OpenShift client. The Spark uploader uses it to read the MinIO credentials secret
(`ibm-lh-minio-secret`) and to open a port-forward to object storage. The `latest/` folder on the
official Red Hat mirror always serves the **newest stable** `oc`.

=== "macOS (Apple Silicon)"

    ```bash
    curl -fsSL -o oc.tar.gz \
      https://mirror.openshift.com/pub/openshift-v4/clients/ocp/latest/openshift-client-mac-arm64.tar.gz
    tar -xzf oc.tar.gz -C ~/.local/bin oc && chmod +x ~/.local/bin/oc
    oc version --client
    ```

    On an Intel Mac, swap `openshift-client-mac-arm64.tar.gz` → `openshift-client-mac.tar.gz`.

=== "Linux"

    ```bash
    # x86-64 (use openshift-client-linux-arm64.tar.gz on ARM64)
    curl -fsSL -o oc.tar.gz \
      https://mirror.openshift.com/pub/openshift-v4/clients/ocp/latest/openshift-client-linux.tar.gz
    tar -xzf oc.tar.gz -C ~/.local/bin oc && chmod +x ~/.local/bin/oc
    oc version --client
    ```

=== "Windows (PowerShell)"

    ```powershell
    Invoke-WebRequest -UseBasicParsing -Uri `
      "https://mirror.openshift.com/pub/openshift-v4/clients/ocp/latest/openshift-client-windows.zip" `
      -OutFile oc.zip
    Expand-Archive oc.zip -DestinationPath "$env:USERPROFILE\bin" -Force
    oc version --client
    ```

    Ensure `%USERPROFILE%\bin` is on your `PATH`.

Expected output:

```text
Client Version: 4.x.x
Kustomize Version: v5.x.x
```

Then log in to the cluster (your administrator provides the API server URL and token/credentials):

```bash
oc login https://api.watson.ibmas-zocp-techcluster.org:6443
```

### 8b · IBM `cpdctl` — needed for the cpdctl path only

`cpdctl` is the IBM Cloud Pak for Data command-line interface. It talks to the watsonx.data
ingestion service directly, bypassing dbt/Spark for the LOAD step (you still use dbt or Spark to
transform the loaded data). cpdctl only INGESTS raw CSV into `lakehouse_demo_ingest` — it is a
loader, not a transformation engine. To build a medallion on top you run the dbt or Spark transforms
against the ingest schema afterward.

These commands resolve the **latest** `cpdctl` release tag from GitHub automatically, then download
the matching binary. Swap the asset name for your platform.

=== "macOS (Apple Silicon)"

    ```bash
    CPDCTL_VERSION=$(curl -fsSL https://api.github.com/repos/IBM/cpdctl/releases/latest \
      | grep -oE '"tag_name": *"[^"]+"' | head -1 | cut -d'"' -f4)
    echo "Latest cpdctl: ${CPDCTL_VERSION}"
    curl -fsSL -o cpdctl.tar.gz \
      "https://github.com/IBM/cpdctl/releases/download/${CPDCTL_VERSION}/cpdctl_darwin_arm64.tar.gz"
    tar -xzf cpdctl.tar.gz -C ~/.local/bin cpdctl && chmod +x ~/.local/bin/cpdctl
    cpdctl version
    ```

=== "Linux"

    ```bash
    # x86-64; use cpdctl_linux_ppc64le / cpdctl_linux_s390x for other arches
    CPDCTL_VERSION=$(curl -fsSL https://api.github.com/repos/IBM/cpdctl/releases/latest \
      | grep -oE '"tag_name": *"[^"]+"' | head -1 | cut -d'"' -f4)
    echo "Latest cpdctl: ${CPDCTL_VERSION}"
    curl -fsSL -o cpdctl.tar.gz \
      "https://github.com/IBM/cpdctl/releases/download/${CPDCTL_VERSION}/cpdctl_linux_amd64.tar.gz"
    tar -xzf cpdctl.tar.gz -C ~/.local/bin cpdctl && chmod +x ~/.local/bin/cpdctl
    cpdctl version
    ```

=== "Windows (PowerShell)"

    ```powershell
    $tag = (Invoke-RestMethod https://api.github.com/repos/IBM/cpdctl/releases/latest).tag_name
    Write-Host "Latest cpdctl: $tag"
    Invoke-WebRequest -UseBasicParsing `
      -Uri "https://github.com/IBM/cpdctl/releases/download/$tag/cpdctl_windows_amd64.tar.gz" `
      -OutFile cpdctl.tar.gz
    tar -xzf cpdctl.tar.gz -C "$env:USERPROFILE\bin" cpdctl.exe
    cpdctl version
    ```

    Ensure `%USERPROFILE%\bin` is on your `PATH`. (`tar` ships with Windows 10+.)

Expected output (version reflects whatever is current):

```text
cpdctl version 1.x.x ...
```

!!! tip "All `cpdctl` assets"
    The full per-platform asset list is on the
    [latest release page](https://github.com/IBM/cpdctl/releases/latest).

After installing the `cpdctl` binary, configure a profile for this workshop environment. The values come from `.env`:

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
