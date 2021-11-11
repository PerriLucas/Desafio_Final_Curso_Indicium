{{ config (materialized='table')}}

with
    product as (
        select
            sk_productid
            , sk_subcategoryid
            , sk_sizeunitmeasureid
            , sk_weightunitmeasureid
            , product_name
            , bought_product
            , avaiable_for_sale
            , color
            , safetystocklevel
            , reorderpoint
            , standardcost 
            , listprice
            , size
            , weight
            , daystomanufacture
            , productline
            , class
            , style
        from {{ ref('stg_product') }}
    )
, product_subcategory as (
    select
        sk_subcategoryid
        , sk_categoryid
        , subcategory_name
    from {{ ref('stg_productsubcategory') }}
)
, product_category as (
    select
        sk_categoryid
        , category_name
    from {{ ref('stg_productcategory') }}
)
, categorization as (
    select 
        product_category.sk_categoryid
        , product_category.category_name
        , product_subcategory.sk_subcategoryid
        , product_subcategory.subcategory_name
    from product_subcategory
    left join product_category on product_category.sk_categoryid=product_subcategory.sk_categoryid
)
, products_categorized as ( 
    select
        product.sk_productid
        , categorization.sk_categoryid
        , categorization.category_name
        , categorization.sk_subcategoryid
        , categorization.subcategory_name
        , product.sk_sizeunitmeasureid
        , product.sk_weightunitmeasureid
        , product.product_name
        , product.bought_product
        , product.avaiable_for_sale
        , product.color
        , product.safetystocklevel
        , product.reorderpoint
        , product.standardcost 
        , product.listprice
        , product.size
        , product.weight
        , product.daystomanufacture
        , product.productline
        , product.class
        , product.style
    from product
    left join categorization on product.sk_subcategoryid=categorization.sk_subcategoryid
)
select * from products_categorized