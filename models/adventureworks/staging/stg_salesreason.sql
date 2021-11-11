with 
  source_data as (
    select 
      salesreasonid
      , reasontype
      , name
      , modifieddate
      from {{ source('adventureworks_etl', 'salesreason')  }}
  )
select * from source_data
