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
, sales_card_customer_location as ( 
    select 
        sales.sk_salesorderid
        , salesperson.sk_businessentityid
        , customer.sk_customerid
        , location.sk_addressid
        , credit_card.sk_creditcardid
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
    left join salesperson on salesperson.sk_businessentityid=sales.sk_salespersonid
    left join customer on customer.sk_customerid = sales.sk_customerid
    left join location on location.sk_addressid = sales.sk_shiptoaddressid
    left join reason on reason.sk_salesorderid = sales.sk_salesorderid
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
, sales_details_product as (
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
        sales_details_product.sk_salesorderdetailid
        , sales_card_customer_location.sk_salesorderid
        , sales_details_product.sk_productid
        , sales_details_product.sk_specialofferid
        , sales_details_product.orderqty
        , sales_details_product.unitprice
        , sales_details_product.unitpricediscount
        , sales_card_customer_location.sk_businessentityid
        , sales_card_customer_location.sk_customerid
        , sales_card_customer_location.sk_addressid
        , sales_card_customer_location.sk_creditcardid
        , sales_card_customer_location.orderdate
        , sales_card_customer_location.shipdate
        , sales_card_customer_location.duedate
        , sales_card_customer_location.salesstatus
        , sales_card_customer_location.isonlineorder
        , sales_card_customer_location.subtotal
        , sales_card_customer_location.taxamt
        , sales_card_customer_location.freight
        , sales_card_customer_location.totaldue
    from sales_details_product
    right join sales_card_customer_location on sales_card_customer_location.sk_salesorderid=sales_details_product.sk_salesorderid
)
select * from sales_final