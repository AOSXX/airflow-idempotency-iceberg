FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "pyiceberg[s3fs,pyarrow]==0.7.1" \
    "pandas==2.1.4" \
    "pyarrow==14.0.2" \
    "psycopg2-binary==2.9.9" \
    "duckdb==0.10.0" \
    "apache-airflow-providers-postgres==5.11.0"
