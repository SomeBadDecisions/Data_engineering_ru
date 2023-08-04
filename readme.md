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

# Информация по проектам:

| Название                                           | Описание                                                                                                | Стек                                            |
|----------------------------------------------------|---------------------------------------------------------------------------------------------------------|-------------------------------------------------|
| [Микросервисная   архитектура и облачные технологии] (https://github.com/SomeBadDecisions/Data_engineering/tree/main/microservices_pg_py_kube/... | Поднятие облачной   инфраструктуры и разработка микросервисов для создания DWH из STG,DDS и CDM   слоёв | Docker, Kubernetes,   Python, PostgreSQL, Redis |
| [DataLake на   Hadoop]                               | Разработка озера   данных на базе Apache Hadoop и автоматизация заполнения слоёв через Apache   Airflow | PySpark, Hadoop,   Airflow                      |
| [Потоковая   обработка данных на SparkStreaming]     | Получение и обработка   сообщений из Kafka, последующая отправка в Postgres и новый топик Kafka         | Kafka,   SparkStreaming, PySpark, PostgreSQL    |
| [Data Vault на   Vertica]                            | Получение данных из   Amazon S3 и создание DWH по модели Data Vault на аналитической БД   Vertica.      | Vertica, Python,   Airflow                      |
| [Проектирование   ETL-пайлайна]                      | Получение данных по   API, их последующая обаботка и сохранение в Postgres                              | MongoDB, PostgreSQL,   Python, Airflow          |
| [Переработка   ETL-пайплайна]                        | Изменение   существующщего пайплайна данных для заполнения DWH в Postgres                               | Airflow, Python,   Postgres                     |
| [Изменение   модели DWH]                             | Изменение   существующей структуры DWH на Postgres                                                      | Postgres                                        |
| [Витрина на   Postgres]                              | Создание RFM-витрины   на Postgres                                                                      | Postgres                                        |
