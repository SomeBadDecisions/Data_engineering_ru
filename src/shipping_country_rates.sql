create table shipping_country_rates (
ID serial,
shipping_country text,
shipping_country_base_rate numeric(14,3),
primary key (ID)
);

create sequence ship_rt_id_seq start 1;

insert into shipping_country_rates
(id, shipping_country, shipping_country_base_rate)
select  
nextval('ship_rt_id_seq') as id  
,shipping_country
,shipping_country_base_rate 
from (select distinct shipping_country, shipping_country_base_rate 
	  from shipping) subq;

drop sequence ship_rt_id_seq;