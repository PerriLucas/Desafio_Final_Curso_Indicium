{{ config (materialized='table')}}

with
    customer as( 
        select
            sk_customerid
            , sk_personid
            , sk_storeid

        from {{ ref('stg_customer') }}

    )
, store as (
    select
        sk_storeid
        , store_name

    from {{ ref('stg_store') }}
)
, person as (
    select
        sk_businessentityid
        , persontype
        , title
        , firstname
        , lastname

    from {{ ref('stg_person') }}
)
, customer_with_store as (
    select
        customer.sk_customerid
        , customer.sk_personid
        , customer.sk_storeid
        , store.sk_storeid
        , store.store_name
    from customer
    full join store on customer.sk_storeid=store.sk_storeid

)
, customer_complete as (
    select
        customer_with_store.sk_customerid
        , customer_with_store.sk_personid
        , customer_with_store.store_name
        , person.sk_businessentityid
        , person.persontype
        , person.title
        , person.firstname
        , person.lastname

    from customer_with_store
    full join person on customer_with_store.sk_personid=person.sk_businessentityid
)
select * from customer_complete
where sk_customerid is not null