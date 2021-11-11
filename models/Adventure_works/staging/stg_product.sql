with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['productid']) }} as sk_productid

       --Foreign Keys
       , {{ dbt_utils.surrogate_key(['productmodelid']) }} as sk_modelid
       , {{ dbt_utils.surrogate_key(['productsubcategoryid']) }} as sk_subcategoryid
       , {{ dbt_utils.surrogate_key(['sizeunitmeasurecode']) }} as sk_sizeunitmeasureid
       , {{ dbt_utils.surrogate_key(['weightunitmeasurecode']) }} as sk_weightunitmeasureid

       --Information
       , name as product_name
       , case 
            when makeflag is true
                then 'Yes' 
                else 'No'
            end as bought_product
        , case
            when finishedgoodsflag is true
                then 'Yes'
                else 'No'
            end as avaiable_for_sale
        , coalesce(color, 'Not Informed') as color
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
    
    from {{ source('desafio_final','product') }} 
    )
    select * from source 