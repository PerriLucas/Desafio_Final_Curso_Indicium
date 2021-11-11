{{ config (materialized='table')}}

with
    salesperson as (
        select
            sk_businessentityid as sk_salespersonid

        from {{ ref('stg_salesperson') }}
    )
    , person as (
        select
            sk_businessentityid
            , title
            , firstname
            , lastname

        from {{ ref('stg_person') }}
    )
, salesperson_info as (
        select
            salesperson.sk_salespersonid
            , person.sk_businessentityid
            , person.title
            , person.firstname
            , person.lastname

        from salesperson
        right join person on salesperson.sk_salespersonid=person.sk_businessentityid
)
select 
    sk_businessentityid
    , title
    , firstname
    , lastname
from salesperson_info