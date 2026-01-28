
SELECT 
{{ dbt_utils.generate_surrogate_key(['p.payment_id']) }} payment_sk ,
p.payment_id, 
c.customer_sk ,
d.date_sk ,
p.payment_method , 
p.payment_status,
p.amount,
p.transaction_time ,
p.is_successful,
p.payment_type,
p.needs_followup,
p.online_payment_status

FROM {{ source("source_silver",'payments')}} p
LEFT JOIN  {{ ref('dim_customers') }}  c
on p.customer_id=c.customer_id
LEFT JOIN {{ ref('dim_date') }} d
    ON p.payment_date = d.date_day
