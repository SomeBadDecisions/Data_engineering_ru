# Построение DWH

## 1.1 Описание
Ранее мною был спроектирован DWH для ресторана, который решил добавить возможность доставки и еды. Новые данные необходимо было корректно собирать и обрабатывать. В качестве источников данных использовались **PostgreSql** и **MongoDB**.
Созданный DWH имеет 3 слоя: 

- staging
- dds
- cdm 

Итоговые DAG'и находятся в /src/dags/existing_dags/examples 

В рамках данного проекта необходимо доработать существующих DWH, добавив новые источники данных и витрину, содерждащие информацию для расчетов с курьерами.
Заказчику необходимо рассчитать суммы оплаты каждому курьеру за предыдущий месяц. Например, в июне выплачивают сумму за май. Расчётным числом является 10-е число каждого месяца.

Основная задача — реализация ETL-процессов, которые будут преобразовывать и перемещать данные от источников до конечных слоёв данных DWH.

## 1.2 Целевая витрина 
Витрина должна называться **cdm.dm_courier_ledger** и содержать следующие поля:

- id — идентификатор записи.
- courier_id — ID курьера, которому перечисляем.
- courier_name — ФИО курьера.
- settlement_year — год отчёта.
- settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
- orders_count — количество заказов за период (месяц).
- orders_total_sum — общая стоимость заказов.
- rate_avg — средний рейтинг курьера по оценкам пользователей.
- order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
- courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
- courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
- courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в текущем месяце:

r < 4 — 5% от заказа, но не менее 100 руб.;

4 <= r < 4.5 — 7% от заказа, но не менее 150 руб.;

4.5 <= r < 4.9 — 8% от заказа, но не менее 175 руб.;

4.9 <= r — 10% от заказа, но не менее 200 руб.

Данные о заказах уже есть в хранилище. Данные курьерской службы необходимо забрать из API курьерской службы, после чего совместить их с данными подсистемы заказов.

## 1.3 Спецификация API 
**API-KEY**: 25c27781-8fde-4b30-a22e-524044a7580f
Для обращения к API необходима утилита curl.

### 1.3.1 GET /restaurants
Метод /restaurants возвращает список доступных ресторанов (_id, name).

```console
curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ offset }}' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 6' \
--header 'X-API-KEY: {{ api_key }}'
# вставить API-KEY без двойных скобок
```

Описание параметров запроса, указанных в {{ }} :

- sort_field — необязательный параметр. Возможные значения: id, name. Значение по умолчанию: id.
Параметр определяет поле, к которому будет применяться сортировка, переданная в параметре sort_direction.
- sort_direction — необязательный параметр. Возможные значения: asc, desc.
Параметр определяет порядок сортировки для поля, переданного в sort_field:
asc — сортировка по возрастанию,
desc — сортировка по убыванию.
- limit — необязательный параметр. Значение по умолчанию: 50. Возможные значения: целое число в интервале от 0 до 50 включительно.
Параметр определяет максимальное количество записей, которые будут возвращены в ответе.
- offset — необязательный параметр. Значение по умолчанию: 0.
Параметр определяет количество возвращаемых элементов результирующей выборки, когда формируется ответ.

### 1.3.2 GET /couriers
Метод /couriers используется для того, чтобы получить список курьеров (_id, name).

```console
curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ offset }}' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 6' \
--header 'X-API-KEY: {{ api_key }}' 
# вставить API-KEY без двойных скобок
```

Параметры /couriers аналогичны параметрам в предыдущем методе: sort_field, sort_direction, limit, offset.

### 1.3.3 GET / deliveries
Метод /deliveries используется для того, чтобы получить список совершённых доставок.

```console
curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?restaurant_id={{ restaurant_id }}&from={{ from }}&to={{ to }}&sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ limit }}' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 6' \
--header 'X-API-KEY: {{ api_key }}'
# вставить API-KEY без двойных скобок
```

Параметры метода также включают sort_field, sort_direction, limit, offset и несколько дополнительных полей:

- sort_field — необязательный параметр. Возможные значения: _id, name. Если указать _id, то сортировка будет применяться к ID заказа (order_id). Если указать date, то сортировка будет применяться к дате создания заказа (order_ts).
- restaurant_id — ID ресторана. Если значение не указано, то метод вернёт данные по всем доступным в БД ресторанам.
- from и to — параметры фильтрации. В выборку попадают заказы с датой доставки между from и to.

Метод возвращает список совершённых доставок с учётом фильтров в запросе. Каждый элемент списка содержит:

- order_id — ID заказа;
- order_ts — дата и время создания заказа;
- delivery_id — ID доставки;
- courier_id — ID курьера;
- address — адрес доставки;
- delivery_ts — дата и время совершения доставки;
- rate — рейтинг доставки, который выставляет покупатель. Целочисленное значение от 1 до 5;
- sum — сумма заказа (в руб.);
- tip_sum — сумма чаевых, которые оставил покупатель курьеру (в руб.).

## 2 Проектирование хранилища

Учитывая кокнретные потребности бизнеса проектирование начнем с итоговогй витрины в cdm-слое.

### 2.1 CDM 

```SQL
create table cdm.dm_courier_ledger (
id serial primary key,
courier_id varchar,
courier_name varchar,
settlement_year integer,
settlement_month integer,
orders_count integer,
orders_total_sum numeric(14,2),
rate_avg numeric(14,2),
order_processing_fee float,
courier_order_sum float,
courier_tips_sum numeric(14,2),
courier_reward_sum float);
```

### 2.2 STG

STG слой будет хранить данные AS IS.

**stg.deliverysystem_couriers**

```SQL
create table stg.deliverysystem_couriers (
id text not NULL,
name varchar
);
```

**stg.deliverysystem_deliveries**

```SQL
create table stg.deliverysystem_deliveries (
order_id text not null,
order_ts timestamp,
delivery_id text not null,
courier_id text not null,
address varchar,
delivery_ts timestamp,
rate numeric(14,2),
sum float,
tip_sum numeric(14,2)
);
```

### 2.3 DDS

В качестве модели данных выбрана "снежинка".

***dds.dim_couriers***

```SQL 
create table dds.dim_couriers(
id serial primary key,
courier_id text,
name varchar
);
```

***dds.fct_courier_tips***

```SQL
create table  dds.fct_courier_tips(
id serial primary key,
courier_id text,
courier_tips_sum numeric(14,2)
);
```

***dds.fct_order_rates***

```SQL
create table  dds.fct_order_rates(
id serial primary key,
order_id text,
order_ts timestamp,
delivery_id text,
address varchar,
delivery_ts timestamp,
courier_id varchar,
rate float,
sum numeric(14,2)
);
```

## 3 Реализация DAG

Далее необходимо реализовать новые DAG'и для корректного извлечения и обработки данных.

### 3.1 STG

Начнем с заливки данных в слой стейджинга.


