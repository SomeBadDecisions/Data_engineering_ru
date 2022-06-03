INSERT INTO analysis.tmp_rfm_frequency (
user_id,
frequency)

SELECT * FROM (
with lt_dt as (
select 
user_id 
, max(order_ts) as latest_order
from analysis.orders o 
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
)subq