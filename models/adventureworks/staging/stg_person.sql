with 
  source_data as (
    select 
      businessentityid
      , firstname
      , middlename
      , lastname
      , persontype
      , title
      , namestyle
      , suffix
      , emailpromotion
      , modifieddate
      , rowguid
      from {{ source('adventureworks_etl', 'person')  }}
  )
select * from source_data