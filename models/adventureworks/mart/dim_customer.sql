{{  config(materialized='table')  }}

with
    personinfo as (
        select businessentityid
        , concat(if(title is null, ' ', person.title), ' ', person.firstname, ' ', person.lastname) as PersonName
        from {{ ref('stg_person') }} person
    )
    , customerinfo as (
        select 
            customerid
          , storeid
          , personid
          , territoryid
          , PersonName
        from {{ ref('stg_customer') }} customer
        left join personinfo on customer.customerid = personinfo.businessentityid
    )

    , customerfinal as (
       select
        row_number() over (order by customerid) as customerSK
        , *
        from customerinfo

    )
select * from customerfinal
