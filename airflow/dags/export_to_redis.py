from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

with DAG(
        'export_to_redis',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        default_args={'retries': 0}
) as dag:

    export_job = SparkSubmitOperator(
        task_id='export_to_redis',
        application='/opt/spark/scripts/export_to_redis.py',
        conn_id='spark_default',
        packages="com.redislabs:spark-redis_2.12:3.1.0",
        conf={'spark.master': 'spark://spark-master:7077'}
    )
    export_job