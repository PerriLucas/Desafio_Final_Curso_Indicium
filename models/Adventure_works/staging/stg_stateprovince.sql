with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['stateprovinceid']) }} as sk_stateprovinceid

       --Foreign Keys
       , {{ dbt_utils.surrogate_key(['countryregioncode']) }} as sk_countryregioncode
       , {{ dbt_utils.surrogate_key(['territoryid']) }} as sk_territoryid

       --Information
       , stateprovincecode
       , name as stateprovincename
       , case 
            when isonlystateprovinceflag is true
                then 'Yes' 
                else 'No'
            end as isonlystateprovince
    

    from {{ source('desafio_final','stateprovince') }} 
    )
    select * from source 