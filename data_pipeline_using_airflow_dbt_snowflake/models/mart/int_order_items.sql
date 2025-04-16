select 
    line_items.order_item_key,
    line_items.line_number,
    line_items.part_key,
    line_items.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discount_amount('line_items.extended_price', 'line_items.discount_percentage')}} as item_discount
from 
{{ref('stg_tcph_sf1_orders')}} as orders
join 
{{ref('stg_tcph_sf1_lineitems')}} as line_items
on orders.order_key = line_items.order_key