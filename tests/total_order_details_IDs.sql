-- Test to see if there was duplication of sales order details IDs in the final fact table.
-- Original value of 121317 duplicated order detail IDs found in the original data set.
with
    test_no_orderdetail_id_repetition as (
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    sk_salesorderdetailid as unique_field,
    count(*) as n_records

from `indicium-328817`.`dbt_Desafio_Final`.`fact_orders`
where sk_salesorderdetailid is not null
group by sk_salesorderdetailid
having count(*) > 1
    )
)
select * from test_no_orderdetail_id_repetition where failures != 121317