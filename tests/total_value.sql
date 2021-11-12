-- Test total value of the sales orders for a given date range. Value of 4610647.2153 extracted from the original dataset.
with 
    unique_orderid as (
        select *
        from (
            select
                sk_salesorderid
                , orderdate
                , totaldue,
                ROW_NUMBER() over (partition by sk_salesorderid order by orderdate desc) rn
            from `indicium-328817.dbt_Desafio_Final.fact_orders`
        ) a
    where rn = 1
    )
, sum_total as (
        select 
            sum(totaldue) as total
        from unique_orderid
        where orderdate between '2012-06-01' and '2012-06-30'
    )
    
select * from sum_total where total != 4610647.2153