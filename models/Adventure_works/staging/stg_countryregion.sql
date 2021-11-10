with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['countryregioncode']) }} as sk_countryregioncode

       --Foreign Keys

       --Information
       , name as countryregioname

    from {{ source('desafio_final','countryregion') }} 
    )
    select * from source 