from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from onedrivesdk.helpers import GetAuthCodeServer

# Importa otras librerías según sea necesario para la autenticación

def get_csv_from_onedrive():
    client_id = Variable.get("onedrive_client_id")  # Obtiene el cliente ID de la Variable en Airflow
    client_secret = Variable.get("onedrive_client_secret")  # Obtiene el cliente secreto de la Variable en Airflow

    redirect_uri = 'http://localhost:8080/'
    client = onedrivesdk.get_default_client(client_id=client_id, scopes=['wl.signin', 'wl.offline_access', 'onedrive.readwrite'])
    
    auth_url = client.auth_provider.get_auth_url(redirect_uri)
    code = GetAuthCodeServer.get_auth_code(auth_url, redirect_uri)
    client.auth_provider.authenticate(code, redirect_uri, client_secret)
    
    # Lógica para descargar el archivo CSV desde OneDrive y guardarlo en una ubicación local en el servidor de Airflow

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 7, 22),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'get_csv_from_onedrive',
    default_args=default_args,
    description='Obtener archivo CSV desde OneDrive',
    schedule_interval='@daily',  # Puedes ajustar esto según la frecuencia deseada
)

get_csv_task = PythonOperator(
    task_id='get_csv_from_onedrive',
    python_callable=get_csv_from_onedrive,
    dag=dag,
)
