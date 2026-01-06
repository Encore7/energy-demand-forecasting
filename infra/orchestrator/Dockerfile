FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    pandas==2.2.2 \
    pyarrow==17.0.0 \
    requests==2.32.3 \
    holidays==0.51 \
    python-dateutil==2.9.0.post0
