with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['salesreasonid']) }} as sk_salesreasonid

       --Foreign Keys

       --Information
       , name 
       , reasontype
    
    from {{ source('desafio_final','salesreason') }} 
    )
    select * from source 