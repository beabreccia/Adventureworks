with 
  source_data as (
    select 
      productcategoryid
      , name
      , modifieddate
      , rowguid
      from {{ source('adventureworks_etl', 'productcategory')  }}
  )
select * from source_data