with 
    sum_total as (
        select 
            sum(totaldue) as total
        from {{ref('fact_orders')}}
        where orderdate between '2012-06-01' and '2012-06-30'
    )
select * from sum_total where total != 262806891.2721