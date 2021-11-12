{{ config (materialized='table')}}

with
    city as (
        select
            sk_addressid
            ,sk_stateprovinceid
            , city
        from {{ ref('stg_address') }}
    )
, state_province as (
    select
        sk_stateprovinceid
        , sk_countryregioncode
        , sk_territoryid
        , stateprovincename
        , isonlystateprovince
    from {{ ref('stg_stateprovince') }}
)
, country_region as (
    select
        sk_countryregioncode
        , countryregionname
    from {{ ref('stg_countryregion') }}
)
, state_complete as (
    select 
         city.sk_addressid
        , city.sk_stateprovinceid
        , city.city
        , state_province.sk_territoryid
        , state_province.stateprovincename
        , state_province.isonlystateprovince
        , state_province.sk_countryregioncode
    from city
    left join state_province on city.sk_stateprovinceid=state_province.sk_stateprovinceid
)
, location as (
    select
        state_complete.sk_addressid
        , state_complete.sk_stateprovinceid
        , state_complete.city
        , state_complete.sk_territoryid
        , state_complete.stateprovincename
        , state_complete.isonlystateprovince
        , country_region.sk_countryregioncode
        , country_region.countryregionname
    from state_complete
    left join country_region on country_region.sk_countryregioncode=state_complete.sk_countryregioncode
)  
select * from location