with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['specialofferid']) }} as sk_specialofferid

       --Foreign Keys

       --Information
       , description
       , discountpct
       , type
       , category
       , startdate
       , enddate
       , maxqty
       , minqty

    
    from {{ source('desafio_final','specialoffer') }} 
    )
    select * from source 