from datetime import datetime, timedelta
from airflow.decorators import dag, task # type: ignore
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='dag_amazon_sales_pipeline_v1',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['amazon', 'etl', 'medallion']
)
def amazon_sales_etl():
    
    @task()
    def ingest_data_to_bigquery():
        # 1. Configuração de caminhos e credenciais
        csv_path = "/opt/airflow/data/amazon_sales.csv"
        key_path = "/opt/airflow/data/service_account.json"

        credentials = service_account.Credentials.from_service_account_file(key_path)
        project_id = credentials.project_id
        table_id = f"{project_id}.amazon_sales.bronze_sales_raw"

        print(f"Iniciando leitura de: {csv_path}")

        # 2. Ler o csv usando o pandas
        df = pd.read_csv(csv_path)

        # Limpeza básica de nomes de colunas (letras minúsculas e sem espaços)
        df.columns = df.columns.str.lower().str.replace(' ', '_')

        print(f"Dados lidos. Shape: {df.shape}. Colunas: {df.columns.tolist()}")

        # 3. Enviar para o BigQuery
        print(f"Enviando {len(df)} linhas para a camada Bronze.")

        # Usando o cliente do BigQuery + Pandas para upload
        # 'replace' = se rodar de novo, apaga e escreve por cima (útil para testes iniciais)
        df.to_gbq(
            destination_table=table_id,
            project_id=project_id,
            credentials=credentials,
            if_exists='replace'
        )
    
        return table_id

    # --------------------------------------------------
    # TASK 2: Camada Silver (Transform & Cleaning)
    # --------------------------------------------------
    @task
    def transform_to_silver(source_table_id):
        key_path = "/opt/airflow/data/service_account.json"
        credentials = service_account.Credentials.from_service_account_file(key_path)
        project_id = credentials.project_id
        target_table = f"{project_id}.amazon_sales.silver_sales_clean"

        print("Extraindo dados da camada Bronze para tranformação.")
        query = f"SELECT * FROM `{source_table_id}`"

        df = pd.read_gbq(query, project_id=project_id, credentials=credentials)

        # Converter colunas de data para o tipo data
        date_columns = [col for col in df.columns if 'date' in col]

        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Remover duplicatas
        df = df.drop_duplicates()

        # Adicionando coluna de controle (auditoria)
        df['extraction_at'] = datetime.now()

        # Tratamento de strings
        df_obj = df.select_dtypes(['object'])
        df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

        # Depois de tratar tudo, envia os dados tratados para o bigquery
        df.to_gbq(
            destination_table=target_table,
            project_id=project_id,
            credentials=credentials,
            if_exists='replace'
        )
        return target_table
    
    # Fluxo de dependências
    bronze_table = ingest_data_to_bigquery()
    transform_to_silver(bronze_table)

# Instância da DAG
main_dag = amazon_sales_etl()