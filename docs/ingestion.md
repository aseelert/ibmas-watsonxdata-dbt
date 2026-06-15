# Native Ingestion (cpdctl)

!!! abstract "What this page adds"
    The dbt and Spark paths both *transform* data. This page shows the third way to
    get raw CSV files **into** watsonx.data: the built-in **ingestion service**, driven
    from the command line with **`cpdctl wx-data ingestion`**. Unlike the other two
    paths, every ingestion job shows up in the watsonx.data console under
    **Data manager → Ingestion (history)**.

## Three ways to load the same CSVs

| Method | Tool | Engine | Shows in UI **Ingestion** history? |
| --- | --- | --- | --- |
| `dbt seed` | dbt → Presto | Presto | No (it is a Presto write) |
| Custom Spark app | `submit_spark_application.py` | Spark | No — appears under **Infrastructure manager → Spark → Applications** |
| **Native ingestion** | **`cpdctl wx-data ingestion`** | Spark | **Yes** — as `ingestion-<id>` / your `--job-id` |

So if you want the load to appear in the **Ingestion** history (the page most people
look at first), use this native ingestion path.

## Prerequisites

1. **Install cpdctl** (the IBM Cloud Pak for Data CLI) — download the build for your OS
   from the [cpdctl releases](https://github.com/IBM/cpdctl/releases) and put it on `PATH`.
   On macOS (Apple silicon):

    ```bash
    curl -fsSL -o cpdctl.tar.gz \
      https://github.com/IBM/cpdctl/releases/download/v1.8.233/cpdctl_darwin_arm64.tar.gz
    tar -xzf cpdctl.tar.gz -C ~/.local/bin && chmod +x ~/.local/bin/cpdctl
    cpdctl version
    ```

2. **Configure a profile** for this instance (values come from your `.env`):

    ```bash
    set -a; source .env; set +a
    cpdctl config profile set wxd-demo \
      --url "https://${WXD_CPD_HOST}" \
      --username "${WXD_CPD_USERNAME}" \
      --apikey "${WXD_API_KEY}" \
      --env "WATSONX_DATA_INSTANCE_ID=${WXD_INSTANCE_ID}"
    ```

3. **Trust the cluster CA.** Point cpdctl at the certificate this repo already manages:

    ```bash
    export SSL_CERT_FILE="$PWD/certs/watsonxdata-ca.pem"
    ```

4. **Stage the CSVs in object storage** (same upload the Spark path uses):

    ```bash
    python scripts/upload_spark_assets.py
    ```

## Run it

```bash
export WXD_CPDCTL_PROFILE=wxd-demo
python scripts/ingest_with_cpdctl.py
```

This creates the target schema (`lakehouse_demo_ingest` by default) and submits one
ingestion job per CSV into `iceberg_data.lakehouse_demo_ingest.<table>`. The script
prints each `job_id` and the console link to watch them.

### The command it runs

For each file it calls, in effect:

```bash
cpdctl wx-data ingestion create \
  --instance-id "${WXD_INSTANCE_ID}" \
  --source-data-files s3://iceberg-bucket/spark_demo/raw/raw_customers.csv \
  --source-file-type csv \
  --target-table iceberg_data.lakehouse_demo_ingest.customers \
  --engine-id "${WXD_SPARK_ENGINE_ID}" \
  --job-id ingest-customers-demo
```

!!! warning "Do not pass `--storage-name` for a registered bucket"
    `iceberg-bucket` is already **registered** in watsonx.data, so the service
    auto-detects it from the `s3://` path. Passing `--storage-name` then triggers the
    *unregistered/transient* storage flow and the job fails with
    `I002 Invalid input provided`. Only set `WXD_INGEST_STORAGE_NAME` (which adds
    `--storage-name`) when ingesting from storage that is **not** registered.

## Watch the jobs

```bash
# list recent ingestion jobs (these are exactly what the UI history shows)
cpdctl wx-data ingestion list --instance-id "${WXD_INSTANCE_ID}" --jobs-per-page 10
```

Or open the console: **Data manager → Ingestion**. Each job moves
`starting → running → FINISHED`. The same files then exist as Iceberg tables you can
query through Presto, e.g. `select * from iceberg_data.lakehouse_demo_ingest.customers`.

!!! note "Engine note"
    Ingestion runs on the **Spark** engine (`spark656` here). Folder ingestion (a whole
    directory of same-shape files into one table) is Spark-only; our four CSVs have
    different shapes, so each is ingested into its own table.
