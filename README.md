# Расширение Data Vault

## 1.1 Описание

Ранее мною было разработано аналитическое хранилище в Vertica для социальной сети. Исходя из потребностей бизнеса была выбрана модель данных **Data Vault**.

Итоговая модель данных на слое DDS выглядит так:

![dds_model](https://user-images.githubusercontent.com/63814959/213483907-d56cda7a-8e62-46cf-a1e3-3d36ba52ae2d.png)

В качестве источника данных использовался **Amazon S3**.

В рамках данного проекта необходимо доработать существующее хранилище. Задача поставлена следующим образом:

Чтобы привлечь новых пользователей, маркетологи хотят разместить на сторонних сайтах рекламу сообществ с высокой активностью. 
Нужно определить группы, в которых начала общаться большая часть их участников. Другими словами, нам нужно выявить группы с самой высокой конверсией.

Пример:

![conversion](https://user-images.githubusercontent.com/63814959/213486274-b85b38e0-7ae4-45f3-8736-18335a9118c3.png)

В группе А конверсия выше, чем в Б. Хотя в группе А сейчас общается только 40 пользователей соцсети, а в Б — 50, доля активных в А выше, ведь в ней всего 50 человек. 
В то время как в группе Б сообщения написали уже 50 участников, но это лишь половина от общего количества — 100. Значит, если в обе группы вступит одинаковое число людей, эффективнее сработает сообщество А, потому что оно лучше вовлекает своих участников. 
Получается, что для рекламы соцсети стоит выбрать группу А и другие паблики с высокой конверсией. Задача — выявить и перечислить маркетологам такие сообщества.


### 1.2  Цель

В качестве источника данных будет использоваться csv-файл, находящийся в хранилище S3.

В рамках проекта необходимо:

- Написать DAG для подключения к S3 и выгрузки файла
- Создать таблицу в Vertica 
- Настроить загрузку AS IS в stg слой 
- Создать хабы, линки, сателлиты на DDS слое и наполнить их 
- Дать ответ бизнесу 

Новые сущности и связи между ними должны выглядеть следующим образом:

![target_dds](https://user-images.githubusercontent.com/63814959/213495667-1712d751-77af-4b9a-840b-1c7f0e346649.png)

## 2.1 Выгрузка из источника 

Первым шагом напишем DAG, который будет забирать csv-файл из S3.

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
import boto3


default_args = {
    'owner': 'Airflow',
    'retries': 0
}

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)



def fetch_s3_file(bucket: str, key: str):
    s3_client.download_file(
        bucket,
        key,
        f'/data/{key}'
    )


with DAG(
    dag_id="group_log",
    default_args=default_args,
    start_date=pendulum.datetime(2022, 12, 25, tz="UTC"),
    schedule_interval=None,
    catchup=False
) as dag:
    
    download = PythonOperator(
        task_id='fetch_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'}
    )

    download 
```

### 2.2 DDL stg  

Далее нам нужно написать DDL для таблицы **group_log** на stg.
На слое stg данные храним AS IS.

```sql
DROP TABLE IF EXISTS RUBTSOV_KA_GMAIL_COM__STAGING.group_log;

CREATE TABLE RUBTSOV_KA_GMAIL_COM__STAGING.group_log (
group_id INT PRIMARY KEY,
user_id INT,
user_id_from INT,
event varchar(40),
datetime datetime
)
segmented by hash(group_id) all nodes
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date,3,2);
```

### 2.3 Загрузка в STG

Дополним созданный ранее DAG задачей на заливку данных из скачанного csv в созданную таблицу **group_log**.

```python 
def load_dataset_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str]
):
    conn_info = {'host': '51.250.75.20',
             'port': 5433,
             'user': 'rubtsov_ka_gmail_com',
             'password': 'etsi1htIOczd'
             }

    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {schema}.{table}")
        cur.execute(f"""COPY {schema}.{table} ({columns})
                   FROM LOCAL '{dataset_path}'
                   DELIMITER ','
                   """)
                   
        conn.commit()
    conn.close()
```

После добавим новую задачу:

```python
to_stg = PythonOperator(
        task_id='csv_to_stg',
        python_callable=load_dataset_to_vertica,
        op_kwargs={'dataset_path': '/data/group_log.csv',
        'schema': 'RUBTSOV_KA_GMAIL_COM__STAGING',
        'table':'group_log',
        'columns':'group_id,user_id,user_id_from,event,datetime'}
    )
```

### 2.4 Создание и наполнение link-таблицы в DWH

Создадим таблицу **l_user_group_activity** в схеме DWH.

Таблица должна содержать следующие поля:

- hk_l_user_group_activity — основной ключ типа INT
- hk_user_id — внешний ключ типа INT, который связан с основным ключом хаба DWH.h_users
- hk_group_id — внешний ключ типа INT, который связан с основным ключом хаба DWH.h_groups
- load_dt — временная отметка типа DATETIME о том, когда были загружены данные
- load_src — данные об источнике типа VARCHAR(20)

```sql
DROP TABLE IF EXISTS RUBTSOV_KA_GMAIL_COM__DWH.l_user_group_activity;

CREATE TABLE RUBTSOV_KA_GMAIL_COM__DWH.l_user_group_activity (
hk_l_user_group_activity INT PRIMARY KEY,
hk_user_id INT NOT NULL CONSTRAINT fk_l_group_user REFERENCES RUBTSOV_KA_GMAIL_COM__DWH.h_users (hk_user_id),
hk_group_id INT NOT NULL CONSTRAINT fk_l_user_group REFERENCES RUBTSOV_KA_GMAIL_COM__DWH.h_groups (hk_group_id),
load_dt DATETIME,
load_src VARCHAR(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
```


Для наполнения таблицы, как и раньше, дополним DAG:

```python 
def stg_to_link(
    schema: str,
    table: str,
    columns: List[str]
):
    conn_info = {'host': '51.250.75.20',
             'port': 5433,
             'user': 'rubtsov_ka_gmail_com',
             'password': 'etsi1htIOczd'
             }

    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {schema}.{table}")
        cur.execute(f"""
        INSERT INTO {schema}.{table}({columns})

        select distinct
        hash(hu.user_id,hg.group_id) as hk_l_user_group_activity
        ,hk_user_id
        ,hk_group_id
        ,now() as load_dt
        ,'S3' as load_src

        from RUBTSOV_KA_GMAIL_COM__STAGING.group_log as gl
        left join RUBTSOV_KA_GMAIL_COM__DWH.h_users hu
            on gl.user_id = hu.user_id 
        left join RUBTSOV_KA_GMAIL_COM__DWH.h_groups hg 
            on gl.group_id = hg.group_id 
        ;
                   """)
                   
        conn.commit()
    conn.close()
```

```python
stg_to_l = PythonOperator(
        task_id='stg_to_l',
        python_callable=stg_to_link,
        op_kwargs={'schema':'RUBTSOV_KA_GMAIL_COM__DWH',
                   'table':'l_user_group_activity',
                   'columns':'hk_l_user_group_activity,hk_user_id,hk_group_id,load_dt,load_src'}
    )
```

### 2.5 Создание и наполнение сателлит-таблицы в DWH

Таблица **s_auth_history** должна содержать поля:

- hk_l_user_group_activity —  внешний ключ к ранее созданной таблице связей типа INT
- user_id_from — идентификатор типа INT того пользователя, который добавил в группу другого. Если новый участник вступил в сообщество сам, это поле пустое 
- event — событие пользователя в группе
- event_dt — дата и время, когда совершилось событие
- load_dt - дата загрузки
- load_src - источник 

```sql
DROP TABLE IF EXISTS RUBTSOV_KA_GMAIL_COM__DWH.s_auth_history;

CREATE TABLE RUBTSOV_KA_GMAIL_COM__DWH.s_auth_history (
hk_l_user_group_activity INT NOT NULL CONSTRAINT fk_activity_auth REFERENCES RUBTSOV_KA_GMAIL_COM__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from INT,
event VARCHAR(40),
event_dt DATETIME,
load_dt DATETIME,
load_src VARCHAR(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
```
