#!/bin/sh
# =============================================================================
#  import-ca-entrypoint.sh — import the OpenShift cluster CA into the JVM
#  cacerts store before starting iceberg-rest-fixture, so that AWS SDK v2
#  (Netty HTTPS client) can verify the MinIO HTTPS Route certificate.
# =============================================================================
set -e

CA_CERT="${CA_CERT_PATH:-/opt/certs/watsonxdata-ca.pem}"
CACERTS="${JAVA_HOME:-/usr/lib/jvm/zulu17-ca-arm64}/lib/security/cacerts"

if [ -f "${CA_CERT}" ]; then
  echo "[entrypoint] Importing CA cert: ${CA_CERT} → ${CACERTS}"
  keytool -importcert \
    -noprompt \
    -trustcacerts \
    -alias watsonxdata-ca \
    -file "${CA_CERT}" \
    -keystore "${CACERTS}" \
    -storepass changeit 2>/dev/null || echo "[entrypoint] CA already imported (skipping)"
  echo "[entrypoint] CA cert import done."
else
  echo "[entrypoint] CA cert not found at ${CA_CERT} — starting without it"
fi

exec java -jar iceberg-rest-adapter.jar
