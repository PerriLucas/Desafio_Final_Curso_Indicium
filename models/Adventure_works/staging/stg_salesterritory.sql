with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['territoryid']) }}  as sk_territoryid

       --Foreign Keys
       , {{ dbt_utils.surrogate_key(['countryregioncode']) }} as sk_countryregioncode

       --Information
       , name
       , salesterritory.group
       , salesytd
       , saleslastyear
       , costytd
       , costlastyear


    from {{ source('desafio_final','salesterritory') }} 
    )
    select * from source 