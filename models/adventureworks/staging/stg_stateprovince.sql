with 
  source_data as (
    select 
      stateprovinceid
      , stateprovincecode
      , territoryid
      , countryregioncode
      , name
      , isonlystateprovinceflag
      , modifieddate
      , rowguid
      from {{  source('adventureworks_etl', 'stateprovince')  }}
  )
select * from source_data
