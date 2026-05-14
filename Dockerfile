FROM apache/airflow:2.10.2

USER root

# 1. Installiamo Java e le librerie Kerberos
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-jre-headless \
    procps \
    wget \
    libkrb5-dev \
    krb5-user \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# 2. Copiamo e prepariamo Spark (Assicurati che non ci sia lo slash iniziale!)
COPY spark_for_airflow/spark-3.5.1-bin-hadoop3.tgz /opt/

RUN tar -xzf /opt/spark-3.5.1-bin-hadoop3.tgz -C /opt/ && \
    rm /opt/spark-3.5.1-bin-hadoop3.tgz && \
    ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark

USER airflow
# 3. Installazione pacchetti Python con Constraints
# Definiamo le versioni per comporre l'URL dei constraints
ARG PYTHON_VERSION=3.12
ARG AIRFLOW_VERSION=2.10.2

# Installiamo tutto in un colpo solo senza aggiornare pip
RUN pip install --no-cache-dir \
    "nipyapi" \
    "apache-airflow-providers-apache-hdfs" \
    "apache-airflow-providers-apache-spark" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"