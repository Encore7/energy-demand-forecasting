FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    ca-certificates \
    git \
    build-essential \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

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
      "requests>=2.32.0"

ENV PYTHONPATH=/opt/edf
WORKDIR /opt/airflow
