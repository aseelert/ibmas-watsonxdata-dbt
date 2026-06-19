#!/bin/sh
# ===========================================================================
# Metabase entrypoint wrapper for the watsonx.data medallion demo.
#
# watsonx.data Presto runs behind TLS signed by the cluster CA
# (certs/watsonxdata-ca.pem). The PrestoDB JDBC driver that Metabase uses to
# talk to Presto verifies the server certificate against a Java truststore and
# offers NO "skip verification" option — so we must actively trust that CA.
#
# Mirroring the rest of the repo, the CA is read straight from the read-only
# mounted project (/project) and never copied into the image. At startup we
# clone the JVM's default cacerts (so public CAs keep working), import the
# watsonx.data CA into the clone, point the JVM at it, then hand off to
# Metabase's normal launcher.
# ===========================================================================
set -eu

JAVA_HOME="${JAVA_HOME:-/opt/java/openjdk}"
KEYTOOL="$JAVA_HOME/bin/keytool"
SRC_CACERTS="$JAVA_HOME/lib/security/cacerts"

# Resolve the CA the SAME way the rest of the repo does (WXD_SSL_VERIFY is the
# dbt/scripts setting, e.g. "certs/watsonxdata-ca.pem"). Relative paths are read
# from the mounted project; absolute paths are used as-is.
CA_PEM="${WXD_SSL_VERIFY:-certs/watsonxdata-ca.pem}"
case "$CA_PEM" in
  /*) : ;;
  *) CA_PEM="/project/${CA_PEM}" ;;
esac

TRUSTSTORE="/tmp/wxd-truststore.jks"
STOREPASS="changeit"   # the default password of the JVM cacerts we clone

if [ -f "$CA_PEM" ] && [ -x "$KEYTOOL" ] && [ -f "$SRC_CACERTS" ]; then
  echo "[metabase-init] Trusting watsonx.data CA chain from $CA_PEM"
  cp "$SRC_CACERTS" "$TRUSTSTORE"
  # The watsonx.data PEM is a CHAIN (leaf + intermediates + root). keytool
  # -importcert only stores the FIRST cert per file, so split the chain and
  # import each cert under its own alias — otherwise the actual trust anchor
  # (the ingress-operator root) never lands in the store and TLS fails PKIX.
  rm -f /tmp/wxd-ca-*.pem
  awk '/-----BEGIN CERTIFICATE-----/{n++} n>0{print > ("/tmp/wxd-ca-" n ".pem")}' "$CA_PEM"
  i=0
  for cert in /tmp/wxd-ca-*.pem; do
    [ -f "$cert" ] || continue
    i=$((i + 1))
    # Failures are non-fatal so a bad cert never blocks Metabase from booting.
    "$KEYTOOL" -importcert -noprompt -trustcacerts \
      -alias "watsonxdata-ca-$i" -file "$cert" \
      -keystore "$TRUSTSTORE" -storepass "$STOREPASS" >/dev/null 2>&1 \
      && echo "[metabase-init]   trusted cert $i" \
      || echo "[metabase-init]   WARNING: could not import cert $i"
  done
  export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Djavax.net.ssl.trustStore=$TRUSTSTORE -Djavax.net.ssl.trustStorePassword=$STOREPASS"
else
  echo "[metabase-init] WARNING: CA cert not found at $CA_PEM (or keytool missing) — TLS to Presto may fail."
fi

# Hand off to Metabase's normal launcher.
exec /app/run_metabase.sh
