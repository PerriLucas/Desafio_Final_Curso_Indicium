with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['businessentityid']) }} as sk_businessentity

       --Foreign Keys

       --Information
       ,name
    
    from {{ source('desafio_final','store') }} 
    )
    select * from source 