with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['productsubcategoryid']) }} as sk_subcategoryid

       -- Foreign Key
       , {{ dbt_utils.surrogate_key(['productcategoryid']) }} as sk_categoryid

       --Information
       , name as subcategory_name
       , modifieddate 
    
    from {{ source('desafio_final','productsubcategory') }} 
    )
    select * from source 