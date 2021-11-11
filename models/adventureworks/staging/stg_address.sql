with 
  source_data as (
    select 
      addressid
      , city
      , addressline1
      , addressline2
      , stateprovinceid
      , postalcode
      , spatiallocation
      , modifieddate	
      , rowguid   	
    from {{ source('adventureworks_etl', 'address')  }}
  )
select * from source_data