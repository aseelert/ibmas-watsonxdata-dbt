#!/usr/bin/env bash
# =============================================================================
#  build_truststore.sh — make the Docker JVMs trust the OpenShift ingress CA
# -----------------------------------------------------------------------------
#  WHY: the MinIO Route is HTTPS and signed by the cluster's own ingress CA
#  (router-ca). The Iceberg REST service and Flink run in Docker, and their
#  Java does not know that CA — without it S3 calls fail with
#  "PKIX path building failed" (https://) or "S3Exception 302" (http://, since
#  the Route redirects). We keep TLS fully ON and just teach Java the CA.
#
#  WHAT: copies the JDK's default CA bundle out of the wxd-flink image, adds the
#  cluster ingress CA, writes confluent/certs/truststore.jks (git-ignored).
#  Idempotent; re-run any time the cluster CA is rotated.
#
#  CA source (first that works): $WXD_ROUTER_CA_FILE  →  `oc` router-ca secret.
# =============================================================================
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS="$(cd "${HERE}/.." && pwd)/certs"
IMAGE="${FLINK_IMAGE:-wxd-flink:2.3}"
mkdir -p "$CERTS"

CA_PEM="${CERTS}/router-ca.pem"
if [[ -n "${WXD_ROUTER_CA_FILE:-}" && -f "$WXD_ROUTER_CA_FILE" ]]; then
  cp "$WXD_ROUTER_CA_FILE" "$CA_PEM"
elif command -v oc >/dev/null 2>&1 && \
     oc -n openshift-ingress-operator get secret router-ca -o jsonpath='{.data.tls\.crt}' 2>/dev/null \
       | base64 -d > "$CA_PEM" 2>/dev/null && [[ -s "$CA_PEM" ]]; then
  :
else
  echo "[truststore] ERROR: no ingress CA. Log in with 'oc login', or set WXD_ROUTER_CA_FILE to a PEM file." >&2
  exit 1
fi
openssl x509 -in "$CA_PEM" -noout >/dev/null   # sanity: really a certificate

docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "[truststore] ERROR: image $IMAGE not built yet" >&2; exit 1; }
rm -f "${CERTS}/truststore.jks"
docker run --rm --user "$(id -u):$(id -g)" --entrypoint bash -v "${CERTS}:/out" "$IMAGE" -c '
  set -e
  cp "$JAVA_HOME/lib/security/cacerts" /out/truststore.jks
  chmod u+w /out/truststore.jks
  keytool -importcert -noprompt -trustcacerts -alias openshift-ingress-ca \
    -file /out/router-ca.pem -keystore /out/truststore.jks -storepass changeit >/dev/null
'
echo "[truststore] OK → ${CERTS}/truststore.jks (default JDK CAs + OpenShift ingress CA)"
