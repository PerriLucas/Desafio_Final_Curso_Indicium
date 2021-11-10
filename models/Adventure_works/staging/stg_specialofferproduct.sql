with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['specialofferid']) }} as sk_specialofferid

       --Foreign Keys
       , {{ dbt_utils.surrogate_key(['productid']) }} as sk_productid

       --Information
    
    from {{ source('desafio_final','specialofferproduct') }} 
    )
    select * from source 