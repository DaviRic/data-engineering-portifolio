import pandas as pd
import requests
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

JSON_KEY_PATH = os.getenv("JSON_KEY_PATH") # O mesmo que eu coloquei no dbt
PROJECT_ID = os.getenv("PROJECT_ID") # ID que eu coloquei no bigquery
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

def get_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd", # Indica a moeda de referência
        "order": "market_cap_desc", # Define o critério de ordenação. Vai trazer na maior pra menor captalização
        "per_page": 100, # Quantidade de itens por página
        "page": 1, # Pegar somente a primeira página da resposta da requisição
        "sparkline": "false" # Indica se eu vou querer receber um mini gráfico de variação de preço dos últimos 7 dias
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception (f"Erro na API:", {response.status_code})

def load_to_bigquery(data):
    client = bigquery.Client.from_service_account_json(JSON_KEY_PATH) # Minha conexão com o BigQuery

    # Vou criar o meu dataframe com os dados que eu recuperei da minha API
    df = pd.DataFrame(data)

    df['ingested_at'] = pd.Timestamp.now()

    # Subindo os dados para o BigQuery
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Determinando qual tipo de inserção eu vou fazer: se é append ou substituição (no meu caso é append)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    # Eu passo usando o client pois é nele que estão as minhas credenciais para o Data Warehouse
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Sucesso. {len(df)} linhas foram carregadas em {table_ref}")

if __name__=="__main__":
    print("Iniciando a extração da CoinGecko")
    raw_data = get_crypto_data()
    load_to_bigquery(raw_data)