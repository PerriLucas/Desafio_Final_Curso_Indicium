with 
    source as (
        select distinct
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['productcategoryid']) }} as sk_categoryid

       --Information
       , name as category_name
       , modifieddate
    
    from {{ source('desafio_final','productcategory') }} 
    )
    select * from source 