with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['salesorderdetailid']) }} as sk_salesorderdetailid

       --Foreign Keys
       , {{ dbt_utils.surrogate_key(['salesorderid']) }} as sk_salesorderid
       , {{ dbt_utils.surrogate_key(['productid']) }} as sk_productid
       , {{ dbt_utils.surrogate_key(['specialofferid']) }} as sk_specialofferid

       --Information
       , orderqty
       , unitprice
       , unitpricediscount

    from {{ source('desafio_final','salesorderdetail') }} 
    )
    select * from source 