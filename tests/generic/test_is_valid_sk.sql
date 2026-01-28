{% test is_valid_sk(model, column_name) %}

with validation as (
    select
        {{ column_name }} as sk_column
    from {{ model }}
),

validation_errors as (
    
    select sk_column
    from validation
    where sk_column is null

    union all

    
    select sk_column
    from validation
    group by sk_column
    having count(*) > 1
)

select *
from validation_errors

{% endtest %}