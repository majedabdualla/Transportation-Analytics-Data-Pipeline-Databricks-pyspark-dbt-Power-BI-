SELECT 
{{ dbt_utils.generate_surrogate_key(['driver_id', 'dbt_valid_from']) }} as driver_sk,
driver_id , 
full_name ,
phone_number,
city,
vehicle_id,
driver_rating,
last_updated_timestamp,
dbt_valid_from as valid_from,
dbt_valid_to as valid_to , 
case 
        when dbt_valid_to = to_date('9999-12-31') then true
        else false
    end as is_current


FROM {{ ref('scd_drivers') }}