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