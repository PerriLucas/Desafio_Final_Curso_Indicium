with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['businessentityid']) }} as sk_businessentityid

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