with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['salesreasonid']) }} as sk_salesreasonid

       --Foreign Keys
       , {{ dbt_utils.surrogate_key(['salesorderid']) }} as sk_salesorderid

       --Information
    
    from {{ source('desafio_final','salesorderheadersalesreason') }} 
    )
    select * from source 