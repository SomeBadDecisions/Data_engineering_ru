# Витрина RFM

## 1.1. Выясним требования к целевой витрине.

Название витрины: Витрина для RFM-сегментации клиентов в анализе сбыта по лояльности

Расположение витрины : analysis.dm_rfm_segments

Глубина данных: с начала 2021 года

Периодичность обновления: не обновляется

Структура витрины:
	
- user_id - ID клиента
- recency - метрика, сотрирующая клиентов по дате последнего заказа (1-самые давние, 5-заказывали недавно)
- frequency - метрика, сортирующая клиентов по частоте заказов (1-заказывают реже всех, 5-чазе всех)
- monetary - метрика, сортирующая клиентов по сумме заказов (1-самые маленькие суммы, 5-самые крупные)
	
Дополнительная информация: при сборке витрины должны учитываться только заказы в статусе 'closed'

## 1.2. Изучим структуру исходных данных.

В качестве источников будем использовать следующие таблицы и их поля:

**production.orders:**
	
- order_ts - дата и время заказа
- user_id - ID пользователя
- cost - сумма заказа
- order_id - ID заказа
- status - код статуса заказа 
		
**production.orderstatuses:**
	
- id - ID статуса заказа 
- key - наименование статуса заказа 

**production.users:**

- id - ID клиента


## 1.3. Проанализируем качество данных

В полях, используемых для построения витрины нет NULL-ов и дублей, т.к. при построении витрины были установлены ограничения на содержимоей полей.
Для таблицы заданы:
 - PRIMARY KEY
 - NOT NULL constraint
 - Дополнительно задано, что стоимость заказака должна равняться сумме оплаты + бонусам
 
 First Header  | Second Header
------------- | -------------
Content Cell  | Content Cell
Content Cell  | Content Cell

## 1.4.1. Сделаем VIEW для таблиц из базы production (по условию задачи, в коде витрины обращаться можно только к схеме analysis).

```SQL
create or replace view analysis.orders as select * from production.orders;
create or replace view analysis.orderitems as select * from production.orderitems;
create or replace view analysis.orderstatuses as select * from production.orderstatuses;
create or replace view analysis.orderstatuslog as select * from production.orderstatuslog;
create or replace view analysis.products as select * from production.products;
create or replace view analysis.users as select * from production.users;
```

## 1.4.2. Напишем DDL-запрос для создания витрины.

```SQL
CREATE TABLE analysis.dm_rfm_segments (
user_id bigint PRIMARY KEY,
recency smallint NOT NULL,
frequency smallint NOT NULL,
monetary smallint NOT NULL)
```
## 1.4.3 Напишем DDL-запрос для создания промежуточных витрин по каждой метрике

```SQL
CREATE TABLE analysis.tmp_rfm_recency (
 user_id INT NOT NULL PRIMARY KEY,
 recency INT NOT NULL CHECK(recency >= 1 AND recency <= 5)
);
CREATE TABLE analysis.tmp_rfm_frequency (
 user_id INT NOT NULL PRIMARY KEY,
 frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5)
);
CREATE TABLE analysis.tmp_rfm_monetary_value (
 user_id INT NOT NULL PRIMARY KEY,
 monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);
```

## 1.4.4 Напишем SQL-запрос для заполнения промежуточных витрин

**1. tmp_rfm_frequency:**

```SQL 
INSERT INTO analysis.tmp_rfm_frequency (
user_id,
frequency)

SELECT * FROM (
with pre as (
select 
user_id 
,count(order_id) as metric
from analysis.orders t1
inner join analysis.orderstatuses t2 on t1.status = t2.id 
									 and t2.key ='Closed'
where (extract('year' from order_ts)) >= 2021								 
group by user_id 
),

prct as 
(select 
percentile_disc(0.2) within group (order by metric) as "1"
,percentile_disc(0.4) within group (order by metric) as "2"
,percentile_disc(0.6) within group (order by metric) as "3"
,percentile_disc(0.8) within group (order by metric) as "4"
from pre
)

select 
user_id,
case when metric <= (select "1" from prct) then 1
	 when metric >= (select "1" from prct)
	 	and metric <=(select "2" from prct) then 2
	 when metric >= (select "2" from prct)
	 	and metric <=(select "3" from prct) then 3
	 when metric >= (select "3" from prct)
	 	and metric <=(select "4" from prct) then 4
	 else 5 end as metric
from pre 
) subq 
```

**2. tmp_rfm_monetary_value:**

```SQL
INSERT INTO analysis.tmp_rfm_monetary_value (
user_id,
monetary_value)

SELECT * FROM (
with pre as (
select 
user_id 
,sum(cost) as metric
from analysis.orders t1
inner join analysis.orderstatuses t2 on t1.status = t2.id 
									 and t2.key ='Closed'
where (extract('year' from order_ts)) >= 2021								 
group by user_id 
),

prct as 
(select 
percentile_disc(0.2) within group (order by metric) as "1"
,percentile_disc(0.4) within group (order by metric) as "2"
,percentile_disc(0.6) within group (order by metric) as "3"
,percentile_disc(0.8) within group (order by metric) as "4"
from pre
)

select 
user_id,
case when metric <= (select "1" from prct) then 1
	 when metric >= (select "1" from prct)
	 	and metric <=(select "2" from prct) then 2
	 when metric >= (select "2" from prct)
	 	and metric <=(select "3" from prct) then 3
	 when metric >= (select "3" from prct)
	 	and metric <=(select "4" from prct) then 4
	 else 5 end as metric
from pre 
) subq 
```

**3.tmp_rfm_recency:**

```SQL
INSERT INTO analysis.tmp_rfm_recency (
user_id,
recency)

SELECT * FROM (
with lt_dt as (
select 
user_id 
, max(order_ts) as latest_order
from analysis.orders t1
inner join analysis.orderstatuses t2 on t1.status = t2.id 
									 and t2.key ='Closed'
where (extract('year' from order_ts)) >= 2021								 
group by user_id 
),

prct_dt as 
(select 
percentile_disc(0.2) within group (order by latest_order) as "1"
,percentile_disc(0.4) within group (order by latest_order ASC) as "2"
,percentile_disc(0.6) within group (order by latest_order ASC) as "3"
,percentile_disc(0.8) within group (order by latest_order ASC) as "4"
from lt_dt
)

select 
user_id,
case when latest_order <= (select "1" from prct_dt) then 1
	 when latest_order >= (select "1" from prct_dt)
	 	and latest_order <=(select "2" from prct_dt) then 2
	 when latest_order >= (select "2" from prct_dt)
	 	and latest_order <=(select "3" from prct_dt) then 3
	 when latest_order >= (select "3" from prct_dt)
	 	and latest_order <=(select "4" from prct_dt) then 4
	 else 5 end as freq
from lt_dt 
) subq 
```

## 1.4.5 Напишем SQL запрос для создания промежуточной витрины по каждой метрике и её заполнения



```SQL
INSERT INTO analysis.dm_rfm_segments (
user_id,
recency,
frequency,
monetary)

SELECT 
r.user_id 
,r.recency
,f.frequency
,m.monetary_value
FROM analysis.tmp_rfm_recency r
inner join analysis.tmp_rfm_frequency f 
	on r.user_id = f.user_id
inner join analysis.tmp_rfm_monetary_value m
	on r.user_id = m.user_id


```
## 2.1 Изменение структуры витрины

По условиям задачи, во втором задании структура данных в схеме production обновилась: в таблице Orders больше нет поля статуса. А это поле необходимо, потому что для анализа нужно выбрать только успешно выполненные заказы со статусом closed.
Вместо поля с одним статусом разработчики добавили таблицу для журналирования всех изменений статусов заказов — **production.OrderStatusLog.**

Структура таблицы **production.OrderStatusLog:**

- id — синтетический автогенерируемый идентификатор записи,
- order_id — идентификатор заказа, внешний ключ на таблицу production.Orders,
- status_id — идентификатор статуса, внешний ключ на таблицу статусов заказов production.OrderStatuses,
- dttm — дата и время получения заказом этого статуса.

Необходимо внести изменения в то, как формируется представление **analysis.Orders**: вернуть в него поле status. Значение в этом поле должно соответствовать последнему по времени статусу из таблицы **production.OrderStatusLog.**

```SQL
DROP VIEW IF EXISTS analysis.Orders;

ALTER TABLE production.Orders DROP COLUMN status;

CREATE VIEW analysis.Orders AS SELECT * FROM production.Orders;
```

## 2.2 Напишем SQL-запрос для создания и заполнения analysis.orders с учетом всех изменений:

```SQL
DROP VIEW analysis.Orders; 

CREATE OR REPLACE VIEW analysis.Orders AS


with max_dttm AS(
select order_id 
,max(dttm) as max_dttm
from production.orderstatuslog
group by 1
),

order_status as (
select t1.order_id
,t2.status_id 
from max_dttm t1 
inner join production.orderstatuslog t2
	on t1.order_id = t2.order_id 
	and t1.max_dttm = t2.dttm 
)

select t1.order_id
,t1.order_ts
,t1.user_id
,t1.cost
,t2.status_id as status
from production.orders t1
left join order_status t2
	on t1.order_id = t2.order_id
```
