# =============================================================================
#  Flink 1.20 — custom image for the confluent streaming demo
#  Adds: Kafka SQL connector, Iceberg runtime, S3A / Hadoop-AWS JARs
# =============================================================================
FROM flink:1.20-scala_2.12

# Maven Central / Apache release base URLs
ARG MAVEN=https://repo1.maven.org/maven2
ARG FLINK_VER=1.20.5
ARG ICEBERG_VER=1.9.1
ARG KAFKA_CONNECTOR_VER=3.3.0-1.20
ARG HADOOP_AWS_VER=3.3.4
ARG AWS_SDK_VER=1.12.367

# ---------------------------------------------------------------------------
# 1. Kafka SQL connector
# ---------------------------------------------------------------------------
RUN curl -fsSL \
  "${MAVEN}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VER}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VER}.jar" \
  -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VER}.jar

# ---------------------------------------------------------------------------
# 2. Iceberg Flink runtime (includes Iceberg REST catalog + S3FileIO client)
# ---------------------------------------------------------------------------
RUN curl -fsSL \
  "${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VER}/iceberg-flink-runtime-1.20-${ICEBERG_VER}.jar" \
  -o /opt/flink/lib/iceberg-flink-runtime-1.20-${ICEBERG_VER}.jar

# ---------------------------------------------------------------------------
# 3. S3A file system — ships bundled in Flink opt/, just needs activating
# ---------------------------------------------------------------------------
RUN cp /opt/flink/opt/flink-s3-fs-hadoop-${FLINK_VER}.jar /opt/flink/lib/

# ---------------------------------------------------------------------------
# 4. Hadoop AWS S3A connector + AWS SDK bundle (needed for real MinIO writes)
# ---------------------------------------------------------------------------
RUN curl -fsSL \
  "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VER}/hadoop-aws-${HADOOP_AWS_VER}.jar" \
  -o /opt/flink/lib/hadoop-aws-${HADOOP_AWS_VER}.jar

RUN curl -fsSL \
  "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VER}/aws-java-sdk-bundle-${AWS_SDK_VER}.jar" \
  -o /opt/flink/lib/aws-java-sdk-bundle-${AWS_SDK_VER}.jar

# ---------------------------------------------------------------------------
# 5. Classpath so Hadoop S3A picks up all JARs
# ---------------------------------------------------------------------------
ENV HADOOP_CLASSPATH=/opt/flink/lib

# Flink default properties — overridden by compose environment block
ENV FLINK_PROPERTIES="rest.bind-address: 0.0.0.0\nrest.address: 0.0.0.0"
