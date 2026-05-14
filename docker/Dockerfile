FROM apache/airflow:2.10.2

USER root
# Installiamo eventuali dipendenze di sistema se necessarie (opzionale)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Aggiorniamo pip e installiamo con un timeout maggiore
RUN pip install --upgrade pip
RUN pip install --no-cache-dir nipyapi