--Считаем количество уникальных пользователей, которые написали хотя бы одно сообщение
with user_group_messages as (
    SELECT hk_group_id, 
    count(distinct(hk_user_id)) as cnt_users_in_group_with_messages
    FROM RUBTSOV_KA_GMAIL_COM__DWH.l_groups_dialogs lgd
    LEFT JOIN RUBTSOV_KA_GMAIL_COM__DWH.l_user_message lum 
    	ON lgd.hk_message_id = lum.hk_message_id 
    GROUP BY hk_group_id 
 ),

--Считаем общее количество пользователей, вступивших в группы 
user_group_log as (
    select hk_group_id,
    count(distinct(hk_user_id)) as cnt_added_users
    
    FROM RUBTSOV_KA_GMAIL_COM__DWH.l_user_group_activity luga
    LEFT JOIN RUBTSOV_KA_GMAIL_COM__DWH.s_auth_history suh 
    	ON luga.hk_l_user_group_activity = suh.hk_l_user_group_activity 
    WHERE suh.event = 'add'
    AND luga.hk_group_id in (SELECT hk_group_id 
    						 FROM RUBTSOV_KA_GMAIL_COM__DWH.h_groups hg
    						 ORDER BY registration_dt
    						 LIMIT 10)
    GROUP BY hk_group_id 
)

--Считаем конверсию 
select  ugl.hk_group_id,
ugl.cnt_added_users,
ugm.cnt_users_in_group_with_messages,
ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc
;