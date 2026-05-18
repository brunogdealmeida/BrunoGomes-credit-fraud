# ── Stage 1: Spark + extra JARs ───────────────────────────────────────────────
# Shares the same base + RUN commands as spark/Dockerfile so Docker cache hits.
FROM apache/spark:3.5.1-python3 AS spark-with-jars

USER root

RUN apt-get update && apt-get install -y --no-install-recommends wget && \
    rm -rf /var/lib/apt/lists/*

ARG ICEBERG_VERSION=1.5.2
ARG NESSIE_VERSION=0.76.6
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.680

RUN wget -q -P /opt/spark/jars/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# ── Stage 2: Airflow with Spark client ────────────────────────────────────────
FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends procps && \
    rm -rf /var/lib/apt/lists/*

# Bring in Spark binaries and JARs from stage 1 (no separate download)
COPY --from=spark-with-jars /opt/spark /opt/spark
COPY --from=spark-with-jars /opt/java  /opt/java

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=${PATH}:${SPARK_HOME}/bin

USER airflow

COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
