from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime, timedelta
import sys
import os

# ADICIONA O CAMINHO DO VOLUME AO PYTHON
sys.path.append('/opt/airflow/scripts')

# AGORA TENTA IMPORTAR
try:
    from ingest_to_bronze import get_crypto_data, load_to_bigquery # type: ignore
except ImportError:
    # Isso evita que a interface do Airflow quebre totalmente se o arquivo sumir
    print("Aguardando montagem do volume de scripts...")

def run_ingestion():
    raw_data = get_crypto_data()
    load_to_bigquery(raw_data)

dafault_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2026, 4, 5),
#    'retries':1
#    'retry_delay': timedelta(minutes=5)
}

with DAG (
    'finance_crypto_pipeline',
    default_args=dafault_args,
    description="Crypto Pipeline: API -> Bronze -> Silver/Gold (dbt)",
    schedule_interval=None,
    catchup=False
) as dag:
    
    task_bronze_ingestion = PythonOperator(
        task_id="ingestion_api_to_bronze",
        python_callable=run_ingestion
    )
    
    task_dbt_run = BashOperator(
        task_id='dbt_transformation',
        bash_command='dbt run --profiles-dir /opt/airflow/dbt/finance_crypto_etl --project-dir /opt/airflow/dbt/finance_crypto_etl'
    )

    task_dbt_test = BashOperator(
        task_id='dbt_quality_test',
        bash_command="dbt test --profiles-dir /opt/airflow/dbt/finance_crypto_etl --project-dir /opt/airflow/dbt/finance_crypto_etl"
    )

    # Ingestão de dados -> Criação das tabelas -> Teste dos dados da tabela
    task_bronze_ingestion >> task_dbt_run >> task_dbt_test