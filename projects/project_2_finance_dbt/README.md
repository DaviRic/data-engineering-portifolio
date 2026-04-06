# 🪙 Crypto Market Analysis Pipeline

Esse projeto visa monitorara a volatilidade de ativos em tempo real para identificar oportunidades, considerando a mudança frequente dos dados e a necessidade da limpeza desses dados. Nele é demonstrado a construção de um pipeline de dados ponta a ponta para analisar o market share das principais criptomoedas do mercado, consumindo dados reais da API **CoinGecko**.

A evolução tecnica deste projeto focou em orquestração, modularidade e escalabilidade saindo de scripts isolados para um ambiente documentado e conteinerizado.

### Stack Tecnológica
- Python: ingestão de dados (Requests) e carga na camada Bronze.
- Google BigQuery: Data Warehouse para armazenamento e processamento.
- dbt (Data Build Tool): Transformação de dados, testes de qualidade e documentação.
- Apache Airflow: Orquestração do pipeline (DAGs) e automação de tarefas.
- Docker: Conteinerização do ambiente de desenvolvimento.

### Arquitetura do Projeto
O pipeline segue o modelo ELT (Extract, Load & Transform), onde os dados brutos são armazenados no Google BigQuery e transformados usando o poder de processamento da nuvem.

1. Extract: O script consome os dados da API da CoinGecko e faz o `Load` dos dados brutos no dataset referente à camada bronze no Bigquery.
2. Orquestração: O Airflow gerencia o fluxo garantindo que o dbt só inicie as transformações quando a ingestão de dados feita pelo Python é bem sucedida.
3. Transformação: Etapa feita pelo dbt criando as camadas Silver e Gold
   - Silver: Limpeza, tipagem e renomeação de colunas
   - Gold: Modelagem de negócio para cáculo de Market Share

### Fluxo de Execução
Inicalmente o Airflow executa o script `ingest_to_bronze.py` que foi desenvolvido para acessar a API da CoinGecko, pegar os dados e retorná-los em formato JSON, a função resposável por isso é a função `get_crypto_data`. Ainda dentro deste script, é executada a função `load_to_bigquery` que pega esses dados retornados da função `get_crypto_data` e carrega na camada _Bronze_ do BigQuery.

Após isso, o Airflow trigga duas tarefas, uma para criar a camada _Silver_ (Staging) e _Gold_ (Truted) e a outra tarefa vai fazer os testes de qualidade dos dados.

### Como rodar o projeto
1. Clonar o repositório
```
git clone https://github.com/DaviRic/data-engineering-portifolio/edit/main/projects/project_2_finance_dbt/
```
2. Configurar as credenciais
  - Coloque a sua `service_account.json` dentro de `/data` (`service_account` é a chave de altenticação gereda no BigQuery).
  - Configure o `profile.yml` seguindo o exemplo em `profile.yml.example`.

3. Subir o ambiente com Docker
```
docker compose up --build
```

4. Acessar o Airflow
  - Abra `localhost:8080` (usuário: `airflow` / senha: `airflow`)
  - Ative a DAG `finance_crypto_pipeline`. Para essa DAG rodar em um intervalo de tempo específico, é possível setar o _schedule_interval_ da DAG no arquivo `dag_finance_crypto` linha 34.

### Resultados e aprendizados que obtive no desenvolvimento desse projeto
  - **Idempotência**: O pipeline foi desenvolvido para ser executado diversas vezes sem duplicidade ou dados corrompidos.
  - Analytics Engineering: uso de macros no _dbt_ para automação de nomes de schemas (os macros constam no arquivo `generate_schema_name.sql`)
  - Observabilidiade: Monitoramento das tarefas através da interface do Airflow.

### Lineage dos Dados
<img width="1846" height="867" alt="data_lineage" src="https://github.com/user-attachments/assets/c0353cae-4386-463b-b794-66c288f26936" />

### Diagrama da Arquiteruta
![arct_data](https://github.com/user-attachments/assets/09a9476f-f25d-4ee4-906f-ece58505132a)
