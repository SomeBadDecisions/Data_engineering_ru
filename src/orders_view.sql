drop view analysis.Orders; 

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
	