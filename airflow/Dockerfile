# Pinned to the tested version for reproducibility. A floating ':latest' can
# pull a major/minor bump that changes SDK import paths, the auth-manager class,
# or health endpoints and silently break this image.
FROM apache/airflow:3.2.2

COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
