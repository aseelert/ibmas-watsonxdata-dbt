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
# 4. Hadoop JARs (unshaded) — provide HdfsConfiguration and Hadoop classes
# ---------------------------------------------------------------------------
# hadoop-common + hadoop-hdfs-client: real (unshaded) Hadoop classes required
# by Flink's HadoopModuleFactory and flink-s3-fs-hadoop plugin at startup.
# hadoop-aws: S3A connector that bridges hadoop-common to AWS SDK.
RUN curl -fsSL \
  "${MAVEN}/org/apache/hadoop/hadoop-common/${HADOOP_AWS_VER}/hadoop-common-${HADOOP_AWS_VER}.jar" \
  -o /opt/flink/lib/hadoop-common-${HADOOP_AWS_VER}.jar

RUN curl -fsSL \
  "${MAVEN}/org/apache/hadoop/hadoop-hdfs-client/${HADOOP_AWS_VER}/hadoop-hdfs-client-${HADOOP_AWS_VER}.jar" \
  -o /opt/flink/lib/hadoop-hdfs-client-${HADOOP_AWS_VER}.jar

RUN curl -fsSL \
  "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VER}/hadoop-aws-${HADOOP_AWS_VER}.jar" \
  -o /opt/flink/lib/hadoop-aws-${HADOOP_AWS_VER}.jar

# ---------------------------------------------------------------------------
# 5. Iceberg AWS bundle — provides AWS SDK v2 classes for Iceberg S3FileIO
# ---------------------------------------------------------------------------
# iceberg-aws-bundle bundles software.amazon.awssdk (SDK v2) shaded inside,
# satisfying the S3FileIO dependency that iceberg-flink-runtime expects.
RUN curl -fsSL \
  "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VER}/iceberg-aws-bundle-${ICEBERG_VER}.jar" \
  -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VER}.jar

# ---------------------------------------------------------------------------
# 6. Avro + Confluent Schema Registry SQL format
# ---------------------------------------------------------------------------
# Provides the 'avro-confluent' format factory used by every Kafka table in
# silver_jobs.sql. This SQL-uber JAR bundles flink-avro AND the Confluent
# schema-registry client, so no extra dependency JARs are needed. Without it
# Flink fails with: "Could not find any factory for identifier 'avro-confluent'".
RUN curl -fsSL \
  "${MAVEN}/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_VER}/flink-sql-avro-confluent-registry-${FLINK_VER}.jar" \
  -o /opt/flink/lib/flink-sql-avro-confluent-registry-${FLINK_VER}.jar

# ---------------------------------------------------------------------------
# 7. Classpath so Hadoop S3A picks up all JARs
# ---------------------------------------------------------------------------
ENV HADOOP_CLASSPATH=/opt/flink/lib

# Flink default properties — overridden by compose environment block
ENV FLINK_PROPERTIES="rest.bind-address: 0.0.0.0\nrest.address: 0.0.0.0"
