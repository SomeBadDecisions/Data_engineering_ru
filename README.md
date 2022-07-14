# Изменение существующего пайплайна

## 1.1 Описание
В рамках данного проекта необходимо изменить процессы в пайплайне так, чтобы они соответствовали новым задачам бизнеса.
 
Существует витрина **mart.f_sales** со следующими полями:

- id - идентификатор продажи (serial)
- date_id - дата продаж в формате YYYYMMDD (int)
- item_id - идентификатор продукта  (int)
- customer_id - идентификатор клиента  (int)
- сity_id - идентификатор город клиента (int)
- quantity - количество купленного товара (double(10,2))
- amount - сумма покупки (double(10,2))

В ходе развития бизнеса, команда разработки добавила функционал отмены заказов и возврата средств (refunded). Значит, процессы в пайплайне нужно обновить.

Новые инкременты с информацией о продажах приходят по API и содержат статус заказа (shipped/refunded).

## 1.2 Спецификация API 
**API-KEY**: 5f55e6c0-e9e5-4a9c-b313-63c01fc31460
Для обращения к API необходима утилита curl.

## 1.2.1 POST /generate_report
Метод /generate_report инициализирует формирование отчёта. 
Метод возвращает task_id — ID задачи, в результате выполнения которой должен сформироваться отчёт.

Обращение к API с помощью curl:

```console
curl --location --request POST 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/generate_report' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 3' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}' 
# вставить API-KEY без двойных скобок
```

## 1.2.2 GET /get_report
Метод get_report используется для получения отчёта после того, как он будет сформирован на сервере.
Пока отчёт будет формироваться, будет возвращаться статус RUNNING.
Если отчёт сформирован, то метод вернёт статус SUCCESS и report_id.

Обращение к API с помощью curl:

```console
curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_report?task_id={{ task_id }}' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 3' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}'
# вставить API-KEY без двойных скобок 
```

Сформированный отчёт содержит четыре файла:

- custom_research.csv,
- user_orders_log.csv,
- user_activity_log.csv,
- price_log.csv.
Файлы отчетов можно получить по URL из параметра s3_path.

## 1.2.3 GET /get_increment
Метод get_increment используется для получения данных за те даты, которые не вошли в основной отчёт. Дата обязательно в формате 2020-01-22T00:00:00.
Если инкремент сформирован, то метод вернёт статус SUCCESS и increment_id. Если инкремент не сформируется, то вернётся NOT FOUND с описанием причины.

Обращение к API с помощью curl:

```console
curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_increment?report_id={{ report_id }}&date={{ date }}' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 3' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}'
# вставить API-KEY без двойных скобок 
```

Сформированный инкремент содержит четыре файла: 

- custom_research_inc.csv 
- user_orders_log_inc.csv 
- user_activity_log_inc.csv
- price_log_inc.csv
Файлы отчетов можно получить по URL из параметра s3_path.

## 2.1 Добавление статуса заказа
В рамках доработки необходимо: 

- учесть в витрине mart.f_sales статусы shipped и refunded. Все данные в витрине следует считать shipped
- обновить пайплайн с учётом статусов и backward compatibility