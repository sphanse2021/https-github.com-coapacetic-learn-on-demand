with

customers as (

  select * from {{ source('jaffle_shop', 'customers') }}

),

orders as (

  select * from {{ source('jaffle_shop', 'orders') }}

),

payments as (

  select * from {{ source('stripe', 'payment') }}

),

 paid_orders as (select Orders.ID as order_id,
        Orders.USER_ID    as customer_id,
        Orders.ORDER_DATE AS order_placed_at,
            Orders.STATUS AS order_status,
with p as (
    select
        orderid as order_id,
        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid
    from raw.stripe.payment
    where status <> 'fail'
    group by 1
),
paid_orders as (
    select
        orders.id as order_id,
        orders.user_id as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        C.FIRST_NAME    as customer_first_name,
            C.LAST_NAME as customer_last_name
    from orders
    left join (select ORDERID as order_id, max(CREATED) as payment_finalized_date, sum(AMOUNT) / 100.0 as total_amount_paid
from {{ source('stripe', 'payment') }}
where STATUS <> 'fail'
group by 1) p ON orders.ID = p.order_id
left join {{ source('jaffle_shop', 'customers') }} C on orders.USER_ID = C.ID ),

customer_orders 
    as (select C.ID as customer_id
        , min(ORDER_DATE) as first_order_date
        , max(ORDER_DATE) as most_recent_order_date
        , count(ORDERS.ID) AS number_of_orders
    from {{ source('jaffle_shop', 'customers') }} C 
    left join {{ source('jaffle_shop', 'orders') }} as Orders
    on orders.USER_ID = C.ID 
    group by 1)

select
    p.*,
    row_number() over (order by p.order_id) as transaction_seq,
    row_number() over (
        partition by customer_id order by p.order_id
    ) as customer_sales_seq,
    case
        when c.first_order_date = p.order_placed_at then 'new' else 'return'
    end as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
from paid_orders as p
left join customer_orders as c on p.customer_id = c.customer_id
left outer join
(
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders as p
    left join
        paid_orders as t2
        on
            p.customer_id = t2.customer_id
            and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
) as x
on p.order_id = x.order_id
order by order_id
