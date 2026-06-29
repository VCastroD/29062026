-- Executado automaticamente pelo container do PostgreSQL (docker-entrypoint-initdb.d)
-- apenas na primeira inicialização do volume de dados.
--
-- Cria o banco ANALÍTICO, separado do banco de metadados do Airflow.
-- A Connection "postgres_analytics" do Airflow aponta para este banco.
CREATE DATABASE ecommerce_analytics;
