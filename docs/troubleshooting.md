# Troubleshooting

## Python Command Not Found

Use the virtual environment interpreter:

```bash
.venv/bin/python scripts/prepare_watsonx_env.py
```

## Missing API Key

Set the API key in `.env`:

```bash
WXD_API_KEY=<your-software-hub-api-key>
```

The connection JSON does not contain the API key.

## Presto 401 or Instance Token Error

Check:

```bash
python scripts/prepare_watsonx_env.py --overwrite
```

Then verify `.env` contains the correct instance id:

```bash
WXD_INSTANCE_ID=<instance-id-from-connection-json>
```

## TLS or Certificate Errors

Regenerate the certificate file from the connection JSON:

```bash
python scripts/prepare_watsonx_env.py --overwrite
```

Confirm:

```bash
WXD_SSL_VERIFY=certs/watsonxdata-ca.pem
```

## MinIO Endpoint Connection Error

If the object store endpoint is `127.0.0.1`, make sure the OpenShift port-forward is running:

```bash
oc -n cpd-instance port-forward svc/ibm-lh-lakehouse-minio-svc 19000:9000
```

Then retry:

```bash
python scripts/upload_spark_assets.py
```

## Spark Submission Is Only a Dry Run

Set:

```bash
export WXD_SPARK_DRY_RUN=false
```

Then submit:

```bash
python scripts/submit_spark_application.py
```

## Spark Application Not Showing In The watsonx.data History

If you submitted the demo Spark job but cannot find it in the UI, you are probably
looking at the **Ingestion** history. These are two different lists:

| Where you look | What it shows | Does the demo job appear? |
| --- | --- | --- |
| **Data manager → Ingestion → History** | Only jobs created by the built-in **Ingestion** feature (named `ingestion-<id>`). | **No** — by design. |
| **Infrastructure manager → Spark engine `spark656` → Applications** | Every application submitted to the Spark engine, with its state. | **Yes** — look here. |

The demo submits a full **PySpark application** (named `watsonxdata-medallion-demo`)
through the Spark applications API, not through the Ingestion service — so it is tracked
as a **Spark application**, not an ingestion entry. It will not show up under Ingestion
history no matter the state.

Find it under the engine's **Applications** tab, or from the command line:

```bash
# the submit script prints the application id and this command for you
python scripts/spark_application_status.py <application-id>
```

Notes:

- Spark application states are `accepted`, `running`, `finished`, `failed`, and `stopped`.
  There is no "approved" state for a Spark application — that belongs to other watsonx.data
  workflows, not Spark submissions.
- The engine runs the job in `deploy_mode: stand-alone`; the per-application Spark UI and
  driver logs are reachable from that Applications tab entry.

If you specifically want a CSV load that **does** appear in the Ingestion history, use the
native ingestion path instead — see [Native Ingestion (cpdctl)](ingestion.md). Those jobs
are created through the ingestion service and show up under Data manager → Ingestion.

## Ingestion Fails With "I002 Invalid input provided"

This usually means `--storage-name` was passed for a bucket that is already **registered**
in watsonx.data (like `iceberg-bucket`). For registered storage, omit `--storage-name` and
let the service detect the bucket from the `s3://` path. Only use `--storage-name` (via
`WXD_INGEST_STORAGE_NAME`) for **unregistered/transient** storage.

## MkDocs Port Already in Use

Use another port:

```bash
mkdocs serve -a 127.0.0.1:8001
```

