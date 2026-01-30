SElECT * 
FROM 
{{ref('fact_trips')}} 
where trip_start_time > trip_end_time 