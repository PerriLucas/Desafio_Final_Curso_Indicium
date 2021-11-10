with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['productcategoryid']) }} as sk_category

       --Information
       , name as category_name
       , modifieddate
    
    from {{ source('desafio_final','productcategory') }} 
    )
    select * from source 