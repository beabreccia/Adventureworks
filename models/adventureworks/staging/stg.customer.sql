with 
  source_data as (
    select 
      customerid
      , storeid
      , personid
      , territoryid
      , modifieddate  
      , rowguid
    from {{ source('adventureworks_etl', 'customer')  }}
  )
select * from source_data