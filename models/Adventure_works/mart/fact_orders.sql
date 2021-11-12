{{ config (materialized='table')}}

with
    credit_card as (
        select
        *
        from {{ ref('dim_card') }}
    )
, location as (
    select
    *
    from {{ ref('dim_location') }}
)
, products as (
    select
    *
    from {{ ref('dim_products') }}
)
, reason as (
    select
    *
    from {{ ref('dim_reason') }}
)
, salesperson as (
    select
    *
    from {{ ref('dim_salesperson') }}
)
, customer as (
    select
    *
    from {{ ref( 'dim_customer') }}
)
, sales as (
    select
        sk_salesorderid
        , sk_customerid
        , sk_salespersonid
        , sk_territoryid
        , sk_creditcardid
        , sk_billtoaddressid
        , sk_shiptoaddressid -- The shipping address will be considered for determining the sale location, as opposed to the billing adress
        , orderdate 
        , shipdate
        , duedate
        , salesstatus
        , isonlineorder
        , subtotal
        , taxamt
        , freight
        , totaldue
    from {{ ref('stg_salesorderheader') }}
)
, sales_added as ( 
    select 
        sales.sk_salesorderid
        , customer.sk_customerid
        , salesperson.sk_businessentityid
        , credit_card.sk_creditcardid
        , location.sk_addressid
        , sales.orderdate
        , sales.shipdate
        , sales.duedate
        , sales.salesstatus
        , sales.isonlineorder
        , sales.subtotal
        , sales.taxamt
        , sales.freight
        , sales.totaldue
    from sales
    left join credit_card on credit_card.sk_creditcardid=sales.sk_creditcardid
    left join customer on customer.sk_customerid=sales.sk_customerid
    left join salesperson on salesperson.sk_businessentityid = sales.sk_salespersonid
    left join location on location.sk_addressid = sales.sk_billtoaddressid
)
, sales_details as (
    select
        sk_salesorderdetailid
        , sk_salesorderid
        , sk_productid
        , sk_specialofferid
        , orderqty
        , unitprice
        , unitpricediscount
    from {{ ref('stg_salesorderdetail') }}
)
, sales_details_added as (
    select
        sales_details.sk_salesorderdetailid
        , sales_details.sk_salesorderid
        , products.sk_productid
        , sales_details.sk_specialofferid
        , sales_details.orderqty
        , sales_details.unitprice
        , sales_details.unitpricediscount
    from sales_details
    left join products on products.sk_productid=sales_details.sk_productid
)
, sales_final as (
    select
        sales_details_added.sk_salesorderdetailid
        , sales_added.sk_salesorderid
        , sales_details_added.sk_productid
        , sales_details_added.sk_specialofferid
        , sales_details_added.orderqty
        , sales_details_added.unitprice
        , sales_details_added.unitpricediscount
        , sales_added.sk_customerid
        , sales_added.sk_businessentityid
        , sales_added.sk_creditcardid
        , sales_added.sk_addressid
        , sales_added.orderdate
        , sales_added.shipdate
        , sales_added.duedate
        , sales_added.salesstatus
        , sales_added.isonlineorder
        , sales_added.subtotal
        , sales_added.taxamt
        , sales_added.freight
        , sales_added.totaldue
    from sales_details_added
    left join sales_added on sales_added.sk_salesorderid=sales_details_added.sk_salesorderid
)
select * from sales_final