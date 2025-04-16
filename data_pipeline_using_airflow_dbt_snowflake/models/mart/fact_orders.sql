select 
orders.*,
order_item_summary.gross_item_sales_amount,
order_item_summary.item_discount_amount
from 
{{ref('stg_tcph_sf1_orders')}} as orders
join 
{{'int_order_item_summary'}} as order_item_summary
on 
orders.order_key = order_item_summary.order_key