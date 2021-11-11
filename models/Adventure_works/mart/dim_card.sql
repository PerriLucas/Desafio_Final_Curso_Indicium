{{ config (materialized='table')}}

with
    card_type as (
        select
            sk_creditcardid
            , cardtype
        from {{ ref('stg_creditcard') }}
    )
select* from card_type