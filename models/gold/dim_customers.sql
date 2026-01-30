SELECT 
{{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_sk,
customer_id , 
full_name,
email,
domain,
phone_number,
city,
signup_date,
last_updated_timestamp,
dbt_valid_from as valid_from,
dbt_valid_to as valid_to , 
case 
        when dbt_valid_to = to_date('9999-12-31') then true
        else false
    end as is_current


FROM {{ ref('scd_customers') }}