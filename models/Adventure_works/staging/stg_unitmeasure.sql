with 
    source as (
        select
        
        -- Primary key
       {{ dbt_utils.surrogate_key(['unitmeasurecode']) }} as sk_unitmeasure

       -- Foreign Key

       --Information
       , name as measure_name
    
    from {{ source('desafio_final','unitmeasure') }} 
    )
    select * from source 