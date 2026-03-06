# 🛒 Amazon Sales Cloud Pipeline

Esse projeto demonstra construção de um pipeline de dados ponta a ponta, utilizando boas práticas de Engenharia de Dados para processar e analisar dados de vendas da Amazon.

## Overview da Arquitetura

O pipeline foi origanizado seguindo a Arquitetura Medalhão, o que garante a linhagem e a qualidade dos dados desde a ingestão até a camada de negócio:

1.  **Ingestão (Bronze):** extração dos dados brutos (csv) e carga no **Google BigQuery**.
2.  **Transformação (Silver):** limpeza, tratamento de tipos e padronização utilizando **Python e Pandas**.
3.  **Agregação (Gold):** modelagem de indicadores de desempenho prontos para consumo (para área área de negócio, por exemplo).

## Tech Stack
- **Orquestração:** Apache Airflow (rodando em Docker)
- **Linguagem:** Python (Pandas, Google Cloud SDK)
- **Data Warehouse:** Google BigQuery
- **Visualização:** Looker Studio
- **Infraestrutura:** Docker & Docker Compose

## Insights Vindo dos Dados
O dashboard final (integrado ao BigQuery) permite a análise de:
- **Performance por Categoria:** Identificação de produtos com maior ticket médio vs. maior volume.
- **Saúde Financeira:** Comparativo entre faturamento bruto e faturamento líquido (ajustado por pedidos cancelados/devolvidos).
- **Tendência Temporal:** Evolução mensal das vendas para identificação de sazonalidade.

## Como Reproduzir
1. Clone o repositório.
2. Baixe a base de dados neste link: https://www.kaggle.com/datasets/rohiteng/amazon-sales-dataset/data
3. Dentro da pasta `project_1_batch_etl` crie a pasta `data` e coloque dentro dela o dataset baixado.
4. Configure sua `service_account.json` do Google Cloud na pasta `/data`.
5. Execute `docker-compose up --build`.
6. Acesse o Airflow em `localhost:8080` e dispare a DAG `amazon_sales_pipeline`.

## Dashboard
[🔗Clique aqui para acessar o dashboard](https://lookerstudio.google.com/reporting/53980437-e27c-4825-b56e-058626fdb02e/page/C3blF)

<img width="757" height="683" alt="image" src="https://github.com/user-attachments/assets/61273eae-5862-48d9-a2d1-dc0001b35895" />
