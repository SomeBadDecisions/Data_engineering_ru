# Витрина RFM

## 1.1. Выясните требования к целевой витрине.

Название витрины: Витрина для RFM-сегментации клиентов в анализе сбыта по лояльности
Расположение витрины : analysis.dm_rfm_segments
Глубина данных: с начала 2021 года
Периодичность обновления: не обновляется
Структура витрины:
	
	- user_id - ID клиента
	- recency - метрика, сотрирующая клиентов по дате последнего заказа (1-самые давние, 5-заказывали недавно)
	- frequency - метрика, сортирующая клиентов по частоте заказов (1-заказывают реже всех, 5-чазе всех)
	- monetary - метрика, сортирующая клиентов по сумме заказов (1-самые маленькие суммы, 5-самые крупные)
	
Дополнительная информация: при сборке витрины учитывались только заказы в статусе 'closed'

## 1.2. Изучите структуру исходных данных.

В качестве источников будем использовать следующие таблицы и их поля:

	production.orders:
	
		- order_ts - дата и время заказа
		- user_id - ID пользователя
		- cost - сумма заказа
		- order_id - ID заказа
		- status - код статуса заказа 
		
	production.orderstatuses:
	
		- id - ID статуса заказа 
		- key - наименование статуса заказа 


## 1.3. Проанализируйте качество данных

В полях, используемых для построения витрины нет NULL-ов и дублей, т.к. при построении витрины были установлены ограничения на содержимоей полей.
Для таблицы заданы:
 - PRIMARY KEY
 - NOT NULL constraint
 - Дополнительно задано, что стоимость заказака должна равняться сумме оплаты + бонусам

### 1.4.1. Сделайте VIEW для таблиц из базы production.**

```SQL
create view analysis.orderitems as select * from production.orderitems;
create view analysis.orderstatuses as select * from production.orderstatuses;
create view analysis.orderstatuslog as select * from production.orderstatuslog;
create view analysis.products as select * from production.products;
create view analysis.users as select * from production.users;
```

### 1.4.2. Напишите DDL-запрос для создания витрины.**

```SQL
CREATE TABLE analysis.dm_rfm_segments (
user_id bigint PRIMARY KEY,
recency smallint NOT NULL,
frequency smallint NOT NULL,
monetary smallint NOT NULL)
```

### 1.4.3. Напишите SQL запрос для заполнения витрины


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




