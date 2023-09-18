# Описание

Этот репозиторий содержит проекты по de.

Стэк:
- **Postgres**
- **Hadoop**
- **pySpark**
- **Kafka**
- **Spark Streaming**
- **Airflow**
- **MongoDb**
- **Vertica**
- **Docker**
- **Kubernetes**
- **Redis**

# Краткая информация по проектам:

| Название                                           | Описание                                                                                                | Стек                                            |
|----------------------------------------------------|---------------------------------------------------------------------------------------------------------|-------------------------------------------------|
| **[Микросервисная   архитектура и облачные технологии](https://github.com/SomeBadDecisions/Data_engineering_ru/tree/main/microservices_pg_py_kube/)** | Поднятие облачной   инфраструктуры и разработка микросервисов для создания DWH из STG, DDS и CDM   слоёв | Docker, Kubernetes,   Python, PostgreSQL, Redis |
| **[DataLake на   Hadoop](https://github.com/SomeBadDecisions/Data_engineering_ru/tree/main/pyspark-hadoop-datalake)**                              | Разработка озера   данных на базе Apache Hadoop и автоматизация заполнения слоёв через Apache   Airflow | PySpark, Hadoop,   Airflow                      |
| **[Потоковая   обработка данных на SparkStreaming](https://github.com/SomeBadDecisions/Data_engineering_ru/tree/main/kafka_sparkstreaming_pipeline)**     | Получение и обработка   сообщений из Kafka, последующая отправка в Postgres и новый топик Kafka         | Kafka,   SparkStreaming, PySpark, PostgreSQL    |
| **[Data Vault на   Vertica](https://github.com/SomeBadDecisions/Data_engineering_ru/tree/main/vertica-data-vault)**                            | Получение данных из   Amazon S3 и создание DWH по модели Data Vault на аналитической БД   Vertica.      | Vertica, Python,   Airflow                      |
| **[Проектирование   ETL-пайлайна](https://github.com/SomeBadDecisions/Data_engineering_ru/tree/main/postgres-mongo-etl-snowflake)**                      | Получение данных по   API, их последующая обаботка и сохранение в Postgres                              | MongoDB, PostgreSQL,   Python, Airflow          |
| **[Переработка   ETL-пайплайна](https://github.com/SomeBadDecisions/Data_engineering_ru/tree/main/postgres-airflow-pipeline-update)**                        | Изменение   существующщего пайплайна данных для заполнения DWH в Postgres                               | Airflow, Python,   Postgres                     |
| **[Изменение   модели DWH](https://github.com/SomeBadDecisions/Data_engineering_ru/tree/main/postgres-datamodel-refactoring)**                             | Изменение   существующей структуры DWH на Postgres                                                      | Postgres                                        |
| **[Витрина на   Postgres](https://github.com/SomeBadDecisions/Data_engineering_ru/tree/main/postgres-rfm-table)**                              | Создание RFM-витрины   на Postgres                                                                      | Postgres                                        |
