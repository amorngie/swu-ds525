select
    t0.id
    ,t0.order_date
    ,t0.status
    ,t1.first_name
    ,t1.last_name
    
from jaffle_shop_orders as t0
join jaffle_shop_customers as t1
on 
    t0.user_id = t1.id
where status = 'completed'