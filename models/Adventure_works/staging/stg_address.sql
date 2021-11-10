with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['addressid']) }} as sk_addressid

       --Foreign Keys
       , {{ dbt_utils.surrogate_key(['stateprovinceid']) }} as sk_stateprovinceid

       --Information
       , city

    from {{ source('desafio_final','address') }} 
    )
    select * from source 