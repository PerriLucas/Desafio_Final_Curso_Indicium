{{ config (materialized='table')}}

with
    sales_order_reason as (
        select
            sk_salesorderid
            , sk_salesreasonid
        from {{ ref('stg_salesorderheadersalesreason') }}
    )
, sales_reason as (
    select
        sk_salesreasonid
        , name
        , reasontype
    from {{ ref('stg_salesreason') }}
)
, sales_reason_final as (
    select
        sales_reason.sk_salesreasonid
        , sales_reason.name
        , sales_reason.reasontype
        , sales_order_reason.sk_salesorderid
    from sales_reason
    left join sales_order_reason on sales_reason.sk_salesreasonid=sales_order_reason.sk_salesreasonid
)
select distinct
    sk_salesorderid
    , sk_salesreasonid
    , name
    , reasontype
 from sales_reason_final
 where sk_salesorderid is not null