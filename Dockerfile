# Imagem customizada do Airflow com as dependências do pipeline.
FROM apache/airflow:2.10.5

# A versão do Python da imagem 2.10.5 é a 3.12.
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12

COPY requirements.txt /requirements.txt

# Instala usando o arquivo de constraints oficial -> evita conflitos de versão
# com o core do Airflow já instalado na imagem base.
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
