with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['creditcardid']) }} as sk_creditcardid

       --Foreign Keys

       --Information
       , cardtype
    
    from {{ source('desafio_final','creditcard') }} 
    )
    select * from source 