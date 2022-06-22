create table shipping_info(
shippingid bigint,
shipping_country_rates_id bigint,
shipping_agreement_id bigint,
shipping_transfer_id bigint,
shipping_plan_datetime timestamp,
payment_amount numeric(14,2),
vendorid bigint,
primary key (shippingid),
foreign key (shipping_country_rates_id) references shipping_country_rates(id) on update cascade,
foreign key (shipping_agreement_id) references shipping_agreement(agreementid) on update cascade,
foreign key (shipping_transfer_id) references shipping_transfer(id) on update cascade
);

insert into shipping_info
(shippingid, shipping_country_rates_id, shipping_agreement_id,
 shipping_transfer_id, shipping_plan_datetime, payment_amount, vendorid)
 select 
 s.shippingid
 ,scr.id as shipping_country_rates_id
 ,sa.agreementid as shipping_agreement_id
 ,st.id as shipping_transfer_id
 ,s.shipping_plan_datetime
 ,s.payment_amount
 ,s.vendorid  
 from shipping s 
 inner join shipping_country_rates scr
 	on s.shipping_country = scr.shipping_country
 inner join shipping_agreement sa 
 	on cast((regexp_split_to_array(s.vendor_agreement_description, E'\\:+'))[1] as integer) = sa.agreementid
 inner join shipping_transfer st 
 	on s.shipping_transfer_description = st.transfer_type || ':' || st.transfer_model
 group by 1,2,3,4,5,6,7