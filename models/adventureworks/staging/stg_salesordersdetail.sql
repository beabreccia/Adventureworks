with 
  source_data as (
    select 
      salesorderdetailid
      , salesorderid
      , specialofferid
      , productid
      , orderqty
      , unitprice
      , unitpricediscount
      , carriertrackingnumber
      , modifieddate
      , rowguid
      from {{ source('adventureworks_etl', 'salesorderdetail')  }}
  )
select * from source_data
