
SELECT 
{{ dbt_utils.generate_surrogate_key(['payment_id']) }} payment_sk ,
p.payment_id, 
c.customer_sk , 
p.payment_method , 
p.payment_status,
p.amount,
p.transaction_time

FROM {{ source("source_silver",'payments')}} p
LEFT JOIN  {{ ref('dim_customers') }}  c
on p.customer_id=c.customer_id
