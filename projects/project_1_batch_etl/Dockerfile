# Imagem estável do airflow
FROM apache/airflow:2.9.0

# Copiando o requirements para dentro do container
COPY requirements.txt .

# Instalação das dependências como usuário Airflow
RUN pip install --no-cache-dir -r requirements.txt