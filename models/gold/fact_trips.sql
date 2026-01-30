WITH cleaned_trips AS (
    SELECT 
        *,
        
        CAST(to_timestamp(trip_start_time) AS DATE) as start_date,
        CAST(to_timestamp(trip_end_time) AS DATE) as end_date
        

    FROM {{ source("source_silver", "trips") }}
)


SELECT 
{{ dbt_utils.generate_surrogate_key(['t.trip_id']) }} trip_sk ,
t.trip_id,
d.driver_sk,
c.customer_sk,
v.vehicle_sk,
dt.date_sk AS start_date_sk,  
dte.date_sk AS end_date_sk,
t.trip_start_time,
t.trip_end_time,
t.start_location,
t.end_location,
t.distance_km ,
t.fare_amount

FROM   cleaned_trips t 
 LEFT JOIN  {{ ref('dim_customers') }}  c
on t.customer_id=c.customer_id
 LEFT JOIN  {{ ref('dim_drivers') }}  d
on t.driver_id=d.driver_id
 LEFT JOIN  {{ ref('dim_vehicles') }}  v
on t.vehicle_id=v.vehicle_id
LEFT JOIN {{ ref('dim_date') }} dt
    ON t.start_date = dt.date_day
LEFT JOIN {{ ref('dim_date') }} dte
    ON t.end_date = dte.date_day

