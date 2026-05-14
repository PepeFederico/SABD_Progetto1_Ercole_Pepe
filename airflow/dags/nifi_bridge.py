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

TARGET_PG_NAME = 'Ingestion_NIFI'


def nifi_start_only():
    """
    Si connette alle API di NiFi, cerca il Process Group specificato e lo avvia.
    Non aspetta la fine del processo, delegando questo compito all'HdfsSensor di Airflow.
    """
    conn = BaseHook.get_connection('nifi_default')
    host = conn.host.replace('https://', '').replace('http://', '')
    api_url = f"https://{host}:{conn.port}/nifi-api"

    print(f"Effettuo login su NiFi: {api_url}")

    auth_data = {
        'username': str(conn.login),
        'password': str(conn.password)
    }

    # Richiesta token
    token_resp = requests.post(
        f"{api_url}/access/token",
        data=auth_data,
        verify=False,
        timeout=10
    )

    if token_resp.status_code != 201:
        print(f"Errore autenticazione NiFi: {token_resp.text}")
        token_resp.raise_for_status()

    headers = {'Authorization': f'Bearer {token_resp.text}'}

    # 2. Ricerca ID del Process Group in base al nome
    search = requests.get(
        f"{api_url}/flow/search-results?q={TARGET_PG_NAME}",
        headers=headers, verify=False
    ).json()

    results = search.get('searchResultsDTO', {}).get('processGroupResults', [])
    pg_id = next((pg['id'] for pg in results if pg['name'] == TARGET_PG_NAME), None)

    if not pg_id:
        raise ValueError(f"Process Group '{TARGET_PG_NAME}' non trovato in NiFi.")

    # 3. Avvio della Pipeline NiFi (Stato: RUNNING)
    requests.put(
        f"{api_url}/flow/process-groups/{pg_id}",
        json={'id': pg_id, 'state': 'RUNNING'},
        headers=headers, verify=False
    ).raise_for_status()

    print(f"Comando di START inviato con successo al Process Group: {pg_id}. Esco senza bloccare.")
    # La funzione termina qui. Il worker di Airflow viene liberato.


with DAG(
        'nifi_spark_pipeline',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        default_args={'retries': 0}
) as dag:
    # TASK 1: Triggeriamo NiFi (fire-and-forget)
    trigger_nifi = PythonOperator(
        task_id='trigger_nifi_ingestion',
        python_callable=nifi_start_only
    )

    # TASK 2: Sensor in attesa del file su HDFS
    wait_for_hdfs_data = WebHdfsSensor(
        task_id='wait_for_avro_files',
        filepath='/data/nifi_output/_SUCCESS',
        webhdfs_conn_id='hdfs_default',
        poke_interval=30,
        timeout=3600,
        mode='reschedule'
    )

    run_spark_job = SparkSubmitOperator(
        task_id='spark_preprocess_avro',
        application='/opt/spark/scripts/spark_preprocessing.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-avro_2.12:3.5.1',
        conf={'spark.master': 'spark://spark-master:7077'}
    )
    
    trigger_nifi >> wait_for_hdfs_data >> run_spark_job