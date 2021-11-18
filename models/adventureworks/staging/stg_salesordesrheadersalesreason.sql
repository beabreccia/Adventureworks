with 
  source_data as (
    select 
      salesorderid
      , salesreasonid
      , modifieddate
      from {{ source('adventureworks_etl', 'salesorderheadersalesreason')  }}
  )
select * from source_data
