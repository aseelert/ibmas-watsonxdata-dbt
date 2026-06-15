# Setup Order

This page gets your laptop ready. Run every command from the repository root:

```bash
cd /Users/aseelert/GitHub/ibmas-watsonxdata-dbt
```

!!! tip "What you are setting up"
    The local machine runs Python helpers and dbt. watsonx.data runs Presto and Spark remotely. MinIO stores the files that Spark reads.

## Step 1: Create Python Environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

This installs:

- dbt and the watsonx.data Presto adapter
- Python clients for Presto, S3/MinIO, and REST calls
- MkDocs and Material for this documentation site

## Step 2: Create dbt Profile

dbt looks for connection profiles in `~/.dbt/profiles.yml`.

```bash
mkdir -p ~/.dbt
cp profiles/profiles.example.yml ~/.dbt/profiles.yml
```

## Step 3: Create Local `.env`

```bash
cp .env.example .env
```

Open `.env` and set your Software Hub API key:

```bash
WXD_API_KEY=<your-software-hub-api-key>
```

!!! warning "Do not commit secrets"
    `.env` is ignored by Git. Keep real API keys only in `.env` or your shell environment.

## Step 4: Add watsonx.data Connection JSON

Export the watsonx.data Presto connection JSON and save it here:

```text
watsonx_data/instance_details.json
```

That JSON contains values such as:

- Presto host and port
- watsonx.data instance id
- CPD / Software Hub host
- SSL certificate chain

It does **not** contain your API key.

## Step 5: Import Connection Values

```bash
python scripts/prepare_watsonx_env.py
```

This updates `.env` with non-secret values and writes the SSL certificate to:

```text
certs/watsonxdata-ca.pem
```

Expected output looks like this:

```text
Read connection details from: watsonx_data/instance_details.json
Wrote certificate chain to: certs/watsonxdata-ca.pem
Updated env file: .env
```

## Step 6: Optional JSON Path

Use this if your exported JSON is somewhere else:

```bash
python scripts/prepare_watsonx_env.py --connection-json /path/to/presto-connection.json
```

## Step 7: Optional Overwrite

Use this when the JSON changed and you want to replace old non-secret values in `.env`:

```bash
python scripts/prepare_watsonx_env.py --overwrite
```

## Step 8: Build The Documentation Site

```bash
mkdocs build --strict
```

Serve the docs locally:

```bash
mkdocs serve -a 127.0.0.1:8001
```

Open:

```text
http://127.0.0.1:8001
```

## Step 9: Continue

Run the dbt path next:

```bash
python scripts/bootstrap_watsonxdata.py
scripts/dbt_env.sh seed --full-refresh
scripts/dbt_env.sh run
scripts/dbt_env.sh test
python scripts/query_gold.py
```

Then run the Spark path:

```bash
python scripts/upload_spark_assets.py
python scripts/submit_spark_application.py
```
