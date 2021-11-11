with 
  source_data as (
    select 
      countryregioncode
      , name
      , modifieddate
      from {{ source('adventureworks_etl', 'countryregion')  }}
  )
select * from source_data