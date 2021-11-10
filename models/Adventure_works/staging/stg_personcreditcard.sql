with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['creditcardid']) }} as sk_creditcardid

       --Foreign Keys
       , {{ dbt_utils.surrogate_key(['businessentityid']) }} as sk_businessentityid

       --Information
    
    from {{ source('desafio_final','personcreditcard') }} 
    )
    select * from source 