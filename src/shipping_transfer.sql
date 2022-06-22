create table shipping_transfer (
id serial,
transfer_type text,
transfer_model text,
shipping_transfer_rate numeric(14,3),
primary key(id)
);

insert into shipping_transfer 
(transfer_type, transfer_model, shipping_transfer_rate)
select distinct 
descr[1] as transfer_type
,descr[2] as transfer_model
,shipping_transfer_rate
from(select 
	 regexp_split_to_array(shipping_transfer_description, E'\\:+') as descr 
	 ,shipping_transfer_rate 
	 from shipping) subq ; 