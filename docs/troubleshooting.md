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

## MkDocs Port Already in Use

Use another port:

```bash
mkdocs serve -a 127.0.0.1:8001
```

