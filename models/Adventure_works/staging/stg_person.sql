with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['businessentityid']) }} as sk_businessentityid

       --Foreign Keys

       --Information
       , persontype
       , title
       , firstname
       , lastname

    
    from {{ source('desafio_final','person') }} 
    )
    select * from source 