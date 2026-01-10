FROM bde2020/spark-base:3.3.0-hadoop3.3 AS spark-base
FROM apache/airflow:2.9.3-python3.11

USER root

# Base utilities + Java
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    ca-certificates \
    git \
    build-essential \
    default-jdk \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Java env
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy Spark from spark-base image
COPY --from=spark-base /spark /opt/spark

RUN mkdir -p /opt/spark/jars

# Added hadoop-aws + aws-java-sdk-bundle jars into Spark's classpath
COPY infra/spark/jars/hadoop-aws-3.3.1.jar /opt/spark/jars/
COPY infra/spark/jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN pip install --upgrade pip setuptools wheel && \
    pip install \
      "apache-airflow-providers-postgres>=5.12.0" \
      "apache-airflow-providers-apache-spark>=4.8.0" \
      "apache-airflow-providers-http>=4.10.0" \
      "great-expectations>=0.18.0" \
      "mlflow>=2.12.0" \
      "feast>=0.40.0" \
      "boto3>=1.34.0" \
      "s3fs>=2024.6.0" \
      "pydantic>=2.7.0" \
      "pydantic-settings>=2.3.0" \
      "python-dotenv>=1.0.0" \
      "requests>=2.32.0" \
      "pyarrow>=16.0.0"

ENV PYTHONPATH=/opt/edf
WORKDIR /opt/airflow
