FROM apache/airflow:2.8.4

USER root

ARG AIRFLOW_VERSION=2.8.4
ARG AIRFLOW_HOME=/opt/airflow

RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN chown -R airflow: ${AIRFLOW_HOME}
USER airflow

COPY ./requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN pip install --no-cache-dir -r ${AIRFLOW_HOME}/requirements.txt

EXPOSE 8080 5555 3306 8812 9000 5432 9009

WORKDIR ${AIRFLOW_HOME}

ENV AIRFLOW_HOME /opt/airflow
