with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['salesorderid']) }} as sk_salesorderid

       --Foreign Keys
       , {{ dbt_utils.surrogate_key(['customerid']) }} as sk_customerid
       , {{ dbt_utils.surrogate_key(['salespersonid']) }} as sk_salespersonid
       , {{ dbt_utils.surrogate_key(['billtoaddressid']) }} as sk_billtoaddressid
       , {{ dbt_utils.surrogate_key(['shiptoaddressid']) }} as sk_shiptoaddressid
       , {{ dbt_utils.surrogate_key(['territoryid']) }} as sk_territoryid
       , {{ dbt_utils.surrogate_key(['creditcardid']) }} as sk_creditcardid

       --Information
       , orderdate --Já estão em formato timestamp 
       , shipdate
       , duedate
       , salesorderheader.status as salesstatus
       , case
            when  onlineorderflag is true
                then 'Yes'
                else 'False'
            end as isonlineorder
        , subtotal
        , taxamt
        , freight
        , totaldue

    from {{ source('desafio_final','salesorderheader') }} 
    )
    select * from source 