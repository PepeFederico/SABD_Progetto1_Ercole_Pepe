from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

with DAG(
        'third_query',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        default_args={'retries': 0}
) as dag:

    run_spark_job_df = SparkSubmitOperator(
        task_id='query3_df',
        application='/opt/spark/scripts/Q3/Q3_df.py',
        conn_id='spark_default',
        conf={'spark.master': 'spark://spark-master:7077'}
    )

    run_spark_job_rdd = SparkSubmitOperator(
        task_id='query3_rdd',
        application='/opt/spark/scripts/Q3/Q3_RDD.py',
        conn_id='spark_default',
        conf={'spark.master': 'spark://spark-master:7077'}
    )

    run_spark_job_df >> run_spark_job_rdd