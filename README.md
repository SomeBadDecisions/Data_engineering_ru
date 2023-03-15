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

### 2.1 Витрина в разрезе пользователей 

Прежде всего необходимо определить, в каком городе было совершено событие.
У нас есть файл geo.csv с координатами центров городов.
Для решения задачи найдем расстояние от координаты отправленного сообщения до центра города по формуле:

![formula](https://user-images.githubusercontent.com/63814959/225387346-3b1cd57e-65fa-4ca3-bfa5-863ea0b11974.png)

где:

- **φ1** — широта первой точки;
- **φ2** — широта второй точки;
- **λ1** — долгота первой точки;
- **λ2** — долгота второй точки;
- **r** — радиус Земли, примерно равный 6371 км.

