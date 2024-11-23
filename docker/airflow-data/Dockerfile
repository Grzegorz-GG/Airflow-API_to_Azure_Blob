ARG AIRFLOW_BASE_IMAGE="apache/airflow:latest"
#"apache/airflow:2.0.0-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

USER root
RUN mkdir -p /data && chown airflow /data

USER airflow

# Install the azure-storage-blob package
RUN pip install --no-cache-dir azure-storage-blob

#USER airflow
