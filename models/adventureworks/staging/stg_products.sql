with 
  source_data as (
    select 
      productid
      , productnumber
      , name
      , class
      , reorderpoint
      , standardcost
      , listprice
      , safetystocklevel
      , size
      , sizeunitmeasurecode
      , weight
      , weightunitmeasurecode
      , color
      , style
      , daystomanufacture
      , productline
      , makeflag
      , finishedgoodsflag
      , sellstartdate
      , sellenddate
      , modifieddate
      , rowguid
      , productsubcategoryid
      , productmodelid
      from {{ source('adventureworks_etl', 'product')  }}
  )

select * from source_data