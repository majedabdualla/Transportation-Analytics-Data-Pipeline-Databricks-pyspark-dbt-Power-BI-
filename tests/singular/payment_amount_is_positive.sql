SELECT 
* 
FROM 
{{ref('fact_payments') }} 
where amount < 0 