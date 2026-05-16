from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# 3. Import di sistema per le chiamate API
import requests
import urllib3

# Disabilita i warning per le richieste HTTPS non verificate
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

with DAG(
        'first_query',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        default_args={'retries': 0}
) as dag:

    run_spark_job = SparkSubmitOperator(
        task_id='spark_preprocess_avro',
        application='/opt/spark/scripts/Q1.py',
        conn_id='spark_default',
        conf={'spark.master': 'spark://spark-master:7077'}
    )

    run_spark_job