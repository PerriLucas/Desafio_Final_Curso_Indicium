with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['customerid']) }} as sk_customerid

       --Foreign Keys
       , {{ dbt_utils.surrogate_key(['personid']) }} as sk_personid
       , {{ dbt_utils.surrogate_key(['storeid']) }} as sk_storeid
       , {{ dbt_utils.surrogate_key(['territoryid']) }} as sk_territoryid

       --Information
    
    from {{ source('desafio_final','customer') }} 
    )
    select * from source 