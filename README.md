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
hdfs dfs -copyFromLocal geo.csv /user/konstantin/data/
```

```python 
geo = spark.read.csv("/user/konstantin/data/geo.csv", sep=';', header= True)\
      .withColumn("lat_r", F.radians(F.col("lat")))\
      .withColumn("lng_r", F.radians(F.col("lng")))\
      .drop("lat")\
      .drop("lng")
```
Далее прочитаем сами данные:

```python
events_geo = spark.read.parquet(events_path) \
    .where("event_type == 'message'")\
    .withColumn("m_lat_r", F.radians(F.col("lat")))\
    .withColumn("m_lng_r", F.radians(F.col("lon")))\
    .withColumn('user_id', F.col('event.message_from'))\
    .withColumn('event_id', F.monotonically_increasing_id())\
	.drop("lat")\
	.drop("lon")
```

После сджойним данные со справочником для последующего вычисления ближайшего центра города и определения home_city:

```python
events_city = events_geo \
                .crossJoin(geo) \
                .withColumn('diff', F.acos(F.sin(F.col('m_lat_r'))*F.sin(F.col('lat_r')) + 
				F.cos(F.col('m_lat_r'))*F.cos(F.col('lat_r'))*F.cos(F.col('m_lng_r')-F.col('lng_r')))*F.lit(6371))
```

Отбросим неподходящие города и оставим только наименее удаленные:

```python
window = Window().partitionBy('event_id').orderBy(F.col('diff'))
real_city = events_city\
            .withColumn("row_number", F.row_number().over(window))\
            .filter(F.col('row_number')==1)\
            .drop("row_number")
```

Далее найдем актуальный адрес, то есть город в котором находился пользователь во время отправки последнего сообщения:

```python
window = Window().partitionBy('user_id').orderBy(F.col("date").desc())
last_city = real_city \
            .withColumn("row_number", F.row_number().over(window)) \
            .filter(F.col("row_number")==1) \
            .selectExpr('user_id', 'city as act_city')
```

Соберем список всех городов, которые посещал пользователь (в порядке посещения):

```python
window = Window().partitionBy('user_id').orderBy(F.col("date"))
cities_list = real_city \
              .withColumn("cities_list", F.collect_list("city").over(window)) \
              .groupBy("user_id").agg(F.max('cities_list'). alias('travel_array'))
```

Найдем домашний город:

```python 
window = Window().partitionBy('user_id', 'id').orderBy(F.col('date'))
 
pre_home = real_city \
    .withColumn("dense_rank", F.dense_rank().over(window)) \
    .withColumn("date_diff", F.datediff(F.col('date').cast(DateType()), F.to_date(F.col("dense_rank").cast("string"), 'd'))) \
    .selectExpr('date_diff', 'user_id', 'date', "id as city_id", "city as home_city" ) \
    .groupBy("user_id", "date_diff", "city_id", "home_city") \
    .agg(F.countDistinct(F.col('date')).alias('cnt_city'))
	
window = Window().partitionBy('user_id')
home_geo = pre_home \
        .withColumn('max_dt', F.max(F.col('date_diff')) \
                .over(window))\
.filter((F.col('cnt_city')>27) & (F.col('date_diff') == F.col('max_dt'))) \
.selectExpr("user_id", "home_city")
```
