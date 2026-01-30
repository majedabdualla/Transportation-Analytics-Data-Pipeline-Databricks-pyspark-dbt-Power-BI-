WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('2021-01-01')",
        end_date="to_date('2027-01-01')"
    ) }}
),

final AS (
    SELECT
        cast(date_day as date) as date_day,
        -- Databricks specific: yyyymmdd is a common integer SK for dates
        cast(date_format(date_day, 'yyyyMMdd') as int) as date_sk,
        year(date_day) as date_year,
        month(date_day) as date_month,
        quarter(date_day) as date_quarter,
        day(date_day) as date_day_of_month,
        date_format(date_day, 'MMMM') as month_name,
        date_format(date_day, 'EEEE') as day_name,
        -- Databricks: 1 (Sunday) to 7 (Saturday)
        case 
            when dayofweek(date_day) in (1, 7) then true 
            else false 
        end as is_weekend,
        -- Useful for filtering current year in dashboards
        case 
            when year(date_day) = year(current_date()) then true 
            else false 
        end as is_current_year
    FROM date_spine
)

SELECT * FROM final