with 
  source_data as (
    select 
      creditcardid
      , cardnumber
      , cardtype
      , expmonth
      , expyear
      , modifieddate
      from {{ source('adventureworks_etl', 'creditcard')  }}
  )

select * from source_data