SELECT 
{{ dbt_utils.generate_surrogate_key(['vehicle_id', 'dbt_valid_from']) }} vehicle_sk,
vehicle_id,
license_plate,
model,
make,
year,
vehicle_type,
last_updated_timestamp,
case 
        when dbt_valid_to = to_date('9999-12-31') then true
        else false
    end as is_current


FROM {{ ref('scd_vehicles') }}
---