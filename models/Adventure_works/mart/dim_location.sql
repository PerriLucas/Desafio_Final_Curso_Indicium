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
, state_province_complete as (
    select 
         city.sk_addressid
        , city.sk_stateprovinceid
        , city.city
        , state_province.sk_territoryid
        , state_province.stateprovincename
        , state_province.isonlystateprovince
        , state_province.sk_countryregioncode
    from state_province
    left join city on city.sk_stateprovinceid=state_province.sk_stateprovinceid
)
, country_complete as (
    select
        state_province_complete.sk_addressid
        , state_province_complete.sk_stateprovinceid
        , state_province_complete.city
        , state_province_complete.sk_territoryid
        , state_province_complete.stateprovincename
        , state_province_complete.isonlystateprovince
        , country_region.sk_countryregioncode
        , country_region.countryregionname
    from state_province_complete
    left join country_region on country_region.sk_countryregioncode=state_province_complete.sk_countryregioncode
)
, sales_territory as (
    select
        sk_territoryid
        , sk_countryregioncode
        , name
    from {{ ref('stg_salesterritory') }}
)
, location as (
    select
        country_complete.sk_addressid
        , country_complete.sk_stateprovinceid
        , country_complete.city
        , sales_territory.sk_territoryid
        , country_complete.stateprovincename
        , country_complete.isonlystateprovince
        , sales_territory.sk_countryregioncode
        , country_complete.countryregionname
    from sales_territory
    left join country_complete on sales_territory.sk_countryregioncode=country_complete.sk_countryregioncode
)
select * from location
where sk_addressid is not null
and sk_stateprovinceid is not null
and sk_territoryid is not null
and sk_countryregioncode is not null
