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