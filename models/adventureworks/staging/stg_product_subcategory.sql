with 
  source_data as (
    select 
      productsubcategoryid
      , productcategoryid
      , name
      , modifieddate
      , rowguid
      from {{ source('adventureworks_etl', 'productsubcategory')  }}
  )
select * from source_data