with 
  source_data as (
    select 
      salesorderid
      , shipmethodid
      , salespersonid
      , billtoaddressid
      , shiptoaddressid
      , territoryid
      , currencyrateid
      , creditcardid
      , customerid
      , purchaseordernumber
      , taxamt
      , onlineorderflag
      , status
      , orderdate
      , creditcardapprovalcode
      , subtotal
      , accountnumber
      , revisionnumber
      , freight
      , duedate
      , shipdate
      , totaldue
      , modifieddate	
      , rowguid
      from {{ source('adventureworks_etl', 'salesorderheader')  }}
  )
select * from source_data