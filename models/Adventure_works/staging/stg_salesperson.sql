with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['businessentityid']) }} as sk_businessentity

       --Foreign Keys
       , {{ dbt_utils.surrogate_key(['territoryid']) }} as sk_territoryid

       --Information
       , salesquota
       , bonus
       , commissionpct
       , salesytd
       , saleslastyear 

    from {{ source('desafio_final','salesperson') }} 
    )
    select * from source 