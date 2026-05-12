from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests, time, urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
TARGET_PG_NAME = 'Ingestion_NIFI'

def nifi_ingestion_manager():
    # 1     Recupero connessione --> Preparazione dell'url ed Autenticazione
    conn = BaseHook.get_connection('nifi_default')
    host = conn.host.replace('https://', '').replace('http://', '')
    api_url = f"https://{host}:{conn.port}/nifi-api"

    print(f"Effettuo login su NiFi: {api_url}")

    # Payload ESPLICITO: NiFi vuole solo questi due campi --> connessione a NiFi
    auth_data = {
        'username': str(conn.login),
        'password': str(conn.password)
    }

    #       A questo punto dobbiamo richiedere un token di accesso a NiFi
    token_resp = requests.post(
        f"{api_url}/access/token",
        data=auth_data,
        verify=False,
        timeout=10
    )

    if token_resp.status_code != 201:
        print(f"Errore NiFi: {token_resp.text}")
        token_resp.raise_for_status()

    headers = {'Authorization': f'Bearer {token_resp.text}'}

    # 2     Ottenuto token --> Ricerca ID del Process Group in modo automatico
    search = requests.get(
        f"{api_url}/flow/search-results?q={TARGET_PG_NAME}",
        headers=headers, verify=False
    ).json()

    results = search.get('searchResultsDTO', {}).get('processGroupResults', [])
    pg_id = next((pg['id'] for pg in results if pg['name'] == TARGET_PG_NAME), None)

    if not pg_id:
        raise ValueError(f"Gruppo {TARGET_PG_NAME} non trovato.")

    # 3     Avvio della Pipeline NiFi (RUNNING)
    requests.put(
        f"{api_url}/flow/process-groups/{pg_id}",
        json={'id': pg_id, 'state': 'RUNNING'},
        headers=headers, verify=False
    ).raise_for_status()
    print(f"NiFi avviato: {pg_id}")

    # 4     Polling (Attesa completamento) --> Perchè NiFi è asincrono. Dobbiamo aspettare che termini scrivendo i file su HDFS altrimenti non possiamo cominciare prossimo task
    timeout = 600
    start_time = time.time()

    while (time.time() - start_time) < timeout:
        status = requests.get(
            f"{api_url}/flow/process-groups/{pg_id}/status",
            headers=headers, verify=False
        ).json()['processGroupStatus']['aggregateSnapshot']

        queued = status['flowFilesQueued']
        active = status['activeThreadCount']

        #       Rappresenta la Barrier --> Controlliamo sia lo stato delle code, che se è presente un Processor attivo
        if queued == 0 and active == 0:
            # 5     Stop della Pipeline NiFi (STOPPED)
            requests.put(
                f"{api_url}/flow/process-groups/{pg_id}",
                json={'id': pg_id, 'state': 'STOPPED'},
                headers=headers, verify=False
            ).raise_for_status()
            print("Lavoro completato. NiFi fermato.")
            return

        time.sleep(15)

    raise TimeoutError("NiFi non ha terminato nel tempo previsto.")

with DAG(
        'nifi_production_clean',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        default_args={'retries': 0}
) as dag:
    run_ingestion = PythonOperator(
        task_id='execute_nifi_flow',
        python_callable=nifi_ingestion_manager
    )