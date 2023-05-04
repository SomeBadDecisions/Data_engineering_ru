# Потоковая обработка данных

## 1.1 Описание
В рамках данного проекта необходимо создать приложение для потоковой обработки данных. Заказчик - агрегатор доставки еды.
В приложении заказчика появилась новая функция - возможность оформления подписки. Подписка дает ряд возможностей, среди которых - добавление понравившегося ресторана в избранное.

От "избранных" ресторанов пользователи будут получать уведомления о различных акциях с ограниченным сроком действия. 

Система работает так:

- Ресторан отправляет через мобильное приложение акцию с ограниченным предложением. Например, такое: «Вот и новое блюдо — его нет в обычном меню. 
Дарим на него скидку 70% до 14:00! Нам важен каждый комментарий о новинке»;
- Сервис проверяет, у кого из пользователей ресторан находится в избранном списке;
- Сервис формирует заготовки для push-уведомлений этим пользователям о временных акциях. Уведомления будут отправляться только пока действует акция.

## 1.2 План проекта

Схема работы сервиса:
![2 _Proekt_1663599076](https://user-images.githubusercontent.com/63814959/236286217-d1d70bc0-9089-45d8-9f11-552d93efe7ee.png)

Для реализации необходимо:

**1. читать данные из Kafka с помощью Spark Structured Streaming и Python в режиме реального времени**

Пример входного сообщения:

```json
first_message:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003",
"adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant.ru",
"adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}
```

где:

- **first_message** — ключ. В рамках задачи он может быть произвольным, но для тестирования кода можно использовать номер сообщения;
- **:** — разделитель ключа и сообщения;
- **"restaurant_id"**: "123e4567-e89b-12d3-a456-426614174000", — UUID ресторана;
- **"adv_campaign_id"**: "123e4567-e89b-12d3-a456-426614174003", — UUID рекламной кампании;
- **"adv_campaign_content"**: "first campaign", — текст кампании;
- **"adv_campaign_owner"**: "Ivanov Ivan Ivanovich", — сотрудник ресторана, который является владельцем кампании;
- **"adv_campaign_owner_contact"**: "iiivanov@restaurant.ru", — его контакт;
- **"adv_campaign_datetime_start"**: 1659203516, — время начала рекламной кампании в формате timestamp;
- **"adv_campaign_datetime_end"**: 2659207116, — время её окончания в формате timestamp;
- **"datetime_created"**: 1659131516 — время создания кампании в формате timestamp.

**2. получать список подписчиков из Postgres**

Таблица содержит соответствия пользователей и ресторанов, на которые они подписаны.

**DDL:**

```sql
CREATE TABLE public.subscribers_restaurants (
    id serial4 NOT NULL,
    client_id varchar NOT NULL,
    restaurant_id varchar NOT NULL,
    CONSTRAINT pk_id PRIMARY KEY (id)
);
```

**3. джойнить данные из Kafka с данными из БД**

**4. сохранять в памяти полученные данные, чтобы не собирать их заново после отправки в Postgres или Kafka**

**5.отправлять выходное сообщение в Kafka с информацией об акции, пользователе со списком избранного и ресторане, а ещё вставлять записи в Postgres, чтобы впоследствии получить фидбэк от пользователя.**

**DDL** целевой таблицы:

```sql 
CREATE TABLE public.subscribers_feedback (
  id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);
```

Структура выходного сообщения:

```json
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003",
"adv_campaign_content":"first campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant.ru",
"adv_campaign_datetime_start":1659203516,"adv_campaign_datetime_end":2659207116,"client_id":"023e4567-e89b-12d3-a456-426614174000",
"datetime_created":1659131516,"trigger_datetime_created":1659304828}
```

По сравнению с входным добавляются два новый поля:
- **"client_id"**:"023e4567-e89b-12d3-a456-426614174000" — UUID подписчика ресторана, который достаётся из таблицы Postgres.
- **"trigger_datetime_created"**:1659304828 — время создания триггера, то есть выходного сообщения. Оно добавляется во время создания сообщения.

**6. На основе полученной информации сервис push-уведомлений будет читать сообщения из Kafka и формировать готовые уведомления.**
Этот шаг происходит уже без нашего участия.






