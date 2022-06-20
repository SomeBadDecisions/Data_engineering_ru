# Изменение модели данных интернет-магазина


**As is:**
<img width="1464" alt="AS_IS" src="https://user-images.githubusercontent.com/63814959/174645196-b37a7492-a730-41e8-8e00-51afb5d657cd.png">

Название основной витрины: shipping
Структура витрины:
	
	- shippingid - уникальный идентификатор доставки.
	- saleid - уникальный идентификатор заказа. К одному заказу может быть привязано несколько строчек shippingid, то есть логов, с информацией о доставке.
	- vendorid - уникальный идентификатор вендора. К одному вендору может быть привязано множество saleid и множество строк доставки.
	- payment - сумма платежа (дублируется).
	- shipping_plan_datetime - плановая дата доставки.
	- status - статус доставки в таблице shipping по данному shippingid. Может принимать значения in_progress — доставка в процессе, либо finished — доставка завершена.
	- state - промежуточные точки заказа, которые изменяются в соответствии с обновлением информации о доставке по времени state_datetime:
		- booked - заказано;
		- fulfillment — заказ доставлен на склад отправки;
		- queued — заказ в очереди на запуск доставки;
		- transition — запущена доставка заказа;
		- pending — заказ доставлен в пункт выдачи и ожидает получения;
		- received — покупатель забрал заказ;
		- returned — покупатель возвратил заказ после того, как его забрал.
	- state_datetime - время обновления состояния заказа.
    - shipping_transfer_description -  строка со значениями transfer_type и transfer_model, записанными через :. Пример записи — 1p:car.
		- transfer_type - тип доставки. 1p означает, что компания берёт ответственность за доставку на себя, 3p — что за отправку ответственен вендор.
		- transfer_model - тип доставки. 1p означает, что компания берёт ответственность за доставку на себя, 3p — что за отправку ответственен вендор.
	- shipping_transfer_rate - процент стоимости доставки для вендора в зависимости от типа и модели доставки, который взимается интернет-магазином для покрытия расходов.
	- shipping_country - страна доставки, учитывая описание тарифа для каждой страны.
	- shipping_country_base_rate - налог на доставку в страну, который является процентом от стоимости payment_amount.
	- vendor_agreement_description - строка, в которой содержатся данные agreementid, agreement_number, agreement_rate, agreement_commission, записанные через разделитель :. Пример записи — 12:vsp-34:0.02:0.023.
		- agreementid - идентификатор договора.
		- agreement_number - номер договора в бухгалтерии.
		- agreement_rate - ставка налога за стоимость доставки товара для вендора.
		- agreement_commission - комиссия, то есть доля в платеже являющаяся доходом компании от сделки.
		
**Пример записей:**
shippingid |	saleid|	orderid	|clientid|	payment|	state_datetime|	productid|	description|	vendorid|	namecategory|	base_country|	status|	state|	shipping_plan_datetime|	hours_to_plan_shipping|	shipping_transfer_description|	shipping_transfer_rate|	shipping_country|shipping_country_base_rate|	vendor_agreement_description
-----------|----------|---------|--------|---------|------------------|----------|-------------|------------|---------------|---------------|---------|------|------------------------|-----------------------|------------------------------|------------------------|-----------------|--------------------------|--------------------------------
1|	1|	4973|	46738|	6.06|	42:34.2|	148|	food&healh vendor_1 from norway|	1|	food&healh|	norway|	in_progress|	booked|	43:42.4|	250.02|	1p:train|	0.025|	russia|	0.03|	0:vspn-4092:0.14:0.02
2|	2|	20857|	192314|	21.93|	27:48.3|	108|	food&healh vendor_1 from germany|	1|	food&healh|	germany|	in_progress|	booked|	49:50.5|	132.37|	1p:train|	0.025|	usa|	0.02|	1:vspn-366:0.13:0.01
3|	3|	14315|	132542|	3.1|	33:16.7|	150|	food&healh vendor_1 from usa|	1|	food&healh|	usa|	in_progress|	booked|	33:16.7|	24|	1p:airplane|	0.04|	norway|	0.04|	2:vspn-4148:0.01:0.01
4|	4|	6682|	61662|	8.57|	21:32.4|	174|	food&healh vendor_3 from russia|	3|	food&healh|	russia|	in_progress|	booked|	14:30.1|	185.87|	1p:train|	0.025|	germany|	0.01|	3:vspn-3023:0.05:0.01
5|	5|	25922|	238974|	1.5|	47:46.1|	135|	food&healh vendor_3 from russia|	3|	food&healh|	russia|	in_progress|	booked|	21:08.8|	102.55|	1p:train|	0.025|	norway|	0.04|	3:vspn-3023:0.05:0.01

**To be:**
<img width="1475" alt="TO_BE" src="https://user-images.githubusercontent.com/63814959/174645334-0c200e6c-a5bd-4963-a2a5-64c1119e5c93.png">
