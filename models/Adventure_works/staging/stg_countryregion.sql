with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['countryregioncode']) }} as sk_countryregioncode

       --Foreign Keys

       --Information
       , name as countryregionname

    from {{ source('desafio_final','countryregion') }} 
    )
    select * from source 