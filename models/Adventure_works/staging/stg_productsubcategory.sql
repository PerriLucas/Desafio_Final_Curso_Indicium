with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['productsubcategoryid']) }} as sk_subcategory

       -- Foreign Key
       , {{ dbt_utils.surrogate_key(['productcategoryid']) }} as sk_category

       --Information
       , name as subcategory_name
       , modifieddate 
    
    from {{ source('desafio_final','productsubcategory') }} 
    )
    select * from source 