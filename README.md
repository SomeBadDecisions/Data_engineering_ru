# Проектирование Data Lake

## 1.1 Описание
В рамках данного проекта необходимо разработать Data Lake для системы рекомендаций социальной сети.
Приложение будет предлагать пользователю написать человеку, если пользователь и адресат:

- состоят в одном канале
- раньше никогда не переписывались
- находятся не дальше 1 км друг от друга

При этом заказчик хочет лучше изучить аудиторию соцсети, чтобы в будущем запустить монетизацию. Для этого было решено провести геоаналитику:

- выяснить, где находится большинство пользователей по количеству сообщений, лайков и подписок из одной точки
- посмотреть, в какой точке Австралии регистрируется больше всего новых пользователей
- определить, как часто пользователи путешествуют и какие города выбирают

Благодаря такой аналитике в соцсеть можно будет вставить рекламу: приложение сможет учитывать местонахождение пользователя и предлагать тому подходящие услуги компаний-партнёров. 

На источнике есть информация о координатах каждого события (сообщение, подписока, реакция, регистрация). 
Данные находятся в hdfs: **/user/master/data/geo/events/**

Также, есть csv-файл с координатами центров городов Австралии для сопоставления. Находится в директории **/src/geo.csv**

### 1.2 Структура хранилища
Для данной задачи будет достаточно 4 слоев:

- **Raw** - здесь хранятся сырые данные. Директория: **/user/master/data/geo/events/date=YYYY-MM-dd**. Данные партиционированы по датам;
- **ODS** - здесь будут хранится предобработанные данные. Директория: **/user/konstantin/data/events/event_type=XXX/date=yyyy-MM-dd**. Здесь данные дополнительно партиционированы по типу события;
- **Sandbox** - этот слой нужен для аналитики и решения Ad hoc задач. Директория: **/user/konstantin/analytics/…**;
- **Data mart** - на этом слое будут хранится итоговые версии витрин данных. Директория: **/user/konstantin/prod/…**.

### 1.3 Целевые витрины

#### 1.3.1 Витрина в разрезе пользователей

Витрина должна содержать следующие поля:

- **user_id** — идентификатор пользователя;
- **act_city** — актуальный адрес. Это город, из которого было отправлено последнее сообщение;
- **home_city** — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней;
- **travel_count** — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение;
- **travel_array** — список городов в порядке посещения;
- **local_time** — местное время события.

#### 1.3.2 Витрина в разрезе зон

В данной витрине должны содержаться события в конкретном городе за неделю и месяц:

- **month** — месяц расчёта;
- **week** — неделя расчёта;
- **zone_id** — идентификатор зоны (города);
- **week_message** — количество сообщений за неделю;
- **week_reaction** — количество реакций за неделю;
- **week_subscription** — количество подписок за неделю;
- **week_user** — количество регистраций за неделю;
- **month_message** — количество сообщений за месяц;
- **month_reaction** — количество реакций за месяц;
- **month_subscription** — количество подписок за месяц;
- **month_user** — количество регистраций за месяц.

#### 1.3.3 Витрина для рекомендации друзей

Данная витрина будет использоваться для рекомендации друзей пользователям. 

Поля витрины:

- **user_left** — первый пользователь;
- **user_right** — второй пользователь;
- **processed_dttm** — дата расчёта витрины;
- **zone_id** — идентификатор зоны (города);
- **local_time** — локальное время.

## 2 Построение витрин

Перед построением витрин заполним ODS-слой. Добавим партиционирование исходных данных по event_type:

```python
events = spark.read.parquet("/user/master/data/geo/events")

events.write.partitionBy("event_type","date")\
.mode("overwrite").parquet("/user/konstantin/data/events")
```

### 2.1 Витрина в разрезе пользователей 

Прежде всего необходимо определить, в каком городе было совершено событие.

У нас есть файл geo.csv с координатами центров городов. Дополнительно добавим туда названия таймзон для последующего определения **local_time**.
Обновленный файл : **/src/geo_timezone.csv** 

Для решения задачи воспользуемся формулой расстояния между двумя точками на сфере:

![image](https://user-images.githubusercontent.com/63814959/226187846-93b00a04-d831-4552-9120-1f18e40516e8.png)

И в данных, и в справочнике широта и долгота указана в градусах. Для этой задачи необходимо перевести значения в радианы.
Загрузим справочник с координатами городов в HDFS и прочиатем его:

```console
hdfs dfs -copyFromLocal geo_timezone.csv /user/konstantin/data/
```

```python 
geo = spark.read.csv(geo_path, sep=';', header= True)\
      .withColumnRenamed("lat", "city_lat")\
      .withColumnRenamed("lng", "city_lon")
```
Далее прочитаем сами данные:

```python
events_geo = spark.read.parquet(events_path) \
    .where("event_type == 'message'")\
    .withColumnRenamed("lat", "msg_lat")\
    .withColumnRenamed("lon","msg_lon")\
    .withColumn('user_id', F.col('event.message_from'))\
    .withColumn('event_id', F.monotonically_increasing_id())
```

Напишем функцию для определения реального города для каждого события. В функции воспользуемся описанной выше формулой расстояния между двумя тчоками:

```python
def get_city(events_geo, geo):

    EARTH_R = 6371

    calculate_diff = 2 * F.lit(EARTH_R) * F.asin(
            F.sqrt(
                F.pow(F.sin((F.radians(F.col("msg_lat")) - F.radians(F.col("city_lat"))) / 2), 2) +
                F.cos(F.radians(F.col("msg_lat"))) * F.cos(F.radians(F.col("city_lat"))) *
                F.pow(F.sin((F.radians(F.col("msg_lon")) - F.radians(F.col("city_lon"))) / 2), 2)
            )
        )

    window = Window().partitionBy('user_id').orderBy(F.col('diff').asc())
    events = events_geo \
            .crossJoin(geo) \
            .withColumn('diff', calculate_diff) \
            .withColumn("row_number", F.row_number().over(window)) \
            .filter(F.col('row_number')==1) \
            .drop('row_number') 
    

    return events
	
	
events = get_city(
    events_geo=events_geo,
    geo=geo
)
```



Далее найдем актуальный адрес, то есть город в котором находился пользователь во время отправки последнего сообщения:

```python
window_act_city = Window().partitionBy('user_id').orderBy(F.col("date").desc())
act_city = events \
            .withColumn("row_number", F.row_number().over(window_last_city)) \
            .filter(F.col("row_number")==1)
```

Найдем список посещенных городов (будем исходить из того, что активность должна быть каждый день):

```python
window_travel = Window().partitionBy('user_id', 'id').orderBy(F.col('date'))
travels = events \
    .withColumn("dense_rank", F.dense_rank().over(window_travel)) \
    .withColumn("date_diff", F.datediff(F.col('date').cast(DateType()), F.to_date(F.col("dense_rank").cast("string"), 'd'))) \
    .selectExpr('date_diff', 'user_id', 'date', "id", "city" ) \
    .groupBy("user_id", "date_diff", "id", "city") \
    .agg(F.countDistinct(F.col('date')).alias('cnt_days'))
	
travels_array = travels.groupBy("user_id") \
    .agg(F.collect_list('city').alias('travel_array')) \
    .select('user_id', 'travel_array', F.size('travel_array').alias('travel_count'))
```

Найдем домашний город:

```python 
home = travels.filter((F.col('cnt_days')>27)) \
    .withColumn('max_dt', F.max(F.col('date_diff')).over(Window().partitionBy('user_id')))\
    .where(F.col('date_diff') == F.col('max_dt')) \
    .selectExpr('user_id', 'city as home_city')
```

Напишем функцию для определения локального времени актуального города:

```python
def calc_local_tm(events):    
    return events.withColumn("TIME",F.col("event.datetime").cast("Timestamp"))\
        .withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone')))\
        .select("local_time", 'user_id')
		
local_time = calc_local_tm(act_city)
```

Все необходимые данные готовы, осталось собрать итоговый результат:

```python
result = events \
        .join(act_city, ['user_id'], 'left') \
        .join(travels_array,['user_id'], 'left') \
        .join(home, ['user_id'], 'left') \
        .join(local_time, ['user_id'], 'left') \
        .selectExpr('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time')
```

Для сборки финальной джобы потребуются 2 функции. 
Первая будет подтягивать спарк-сессию, вторая будет записывать итоговый результат.

Функция для инициализации спарк-сессии (в общем виде, т.к. не известны параметры кластера):

```python
def spark_session_init(name):
    return SparkSession \
        .builder \
        .master("yarn")\
        .appName(f"{name}") \
        .getOrCreate()
```

Функция для записи:
```python 
def write_df(df, df_name, date):
    df.write.mode('overwrite').parquet(f'/user/konstantin/prod/{df_name}/date={date}')
```

Функции вынесем в отдельный файл: **/src/scripts/tools.py**

Финальная джоба: **/src/scripts/dm_users.py** 

Локальный тестовый **.ipynb**: **/src/dm_users.ipynb** 

### 2.1 Витрина в разрезе зон

Создадим витрине с распределением различных событий по городам.
Данная витрина поможет понимать поведение пользователей в зависимости от географической зоны.

Для начала необходимо вновь прочитать данные. 
Этот шаг никак не будет отличаться от предыдущей витрины за исключением того, 
что отбирать будем все события, а не только с типом "сообщение".

Соберем витрину:

```python
w_week = Window.partitionBy(['city', F.trunc(F.col("date"), "week")])
w_month = Window.partitionBy(['city', F.trunc(F.col("date"), "month")])

df_zones = events.withColumn('week_message', F.count(F.when(events.event_type == 'message','event_id')).over(w_week)) \
    .withColumn('week_reaction', F.count(F.when(events.event_type == 'reaction','event_id')).over(w_week)) \
    .withColumn('week_subscription', F.count(F.when(events.event_type == 'subscription','event_id')).over(w_week)) \
    .withColumn('week_user', F.count(F.when(events.event_type == 'registration','event_id')).over(w_week)) \
    .withColumn('month_message', F.count(F.when(events.event_type == 'message','event_id')).over(w_month)) \
    .withColumn('month_reaction', F.count(F.when(events.event_type == 'reaction','event_id')).over(w_month)) \
    .withColumn('month_subscription', F.count(F.when(events.event_type == 'subscription','event_id')).over(w_month)) \
    .withColumn('month_user', F.count(F.when(events.event_type == 'registration','event_id')).over(w_month)) \
    .withColumn('month', F.trunc(F.col("date"), "month")) \
    .withColumn('week', F.trunc(F.col("date"), "week")) \
    .selectExpr('month', 'week', 'id as zone_id', 'week_message', 'week_reaction', 'week_subscription', 'week_user', \
            'month_message', 'month_reaction', 'month_subscription', 'month_user') \
    .distinct()
```

Джоба: **/src/scripts/dm_zone.py**

Локальный тестовый **.ipynb**: **/src/dm_zone.ipynb** 

