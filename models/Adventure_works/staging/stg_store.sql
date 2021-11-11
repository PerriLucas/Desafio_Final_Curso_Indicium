with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['businessentityid']) }} as sk_storeid

       --Foreign Keys

       --Information
       ,name as store_name
    
    from {{ source('desafio_final','store') }} 
    )
    select * from source 