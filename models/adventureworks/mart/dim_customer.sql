{{  config(materialized='table')  }}

with
    customerinfo as (
        select 
            customerid
          , storeid
          , personid
          , territoryid
        from {{ ref('stg_customer') }} customer
    )
    , personinfo as (
        select businessentityid
        , concat(if(title is null, ' ', person.title), ' ', person.firstname, ' ', person.lastname) as PersonName
        from {{ ref('stg_person') }} person
    )
    , customerfinal as (
        select row_number() over (order by customerinfo.customerid) as customerSK
        , personinfo.businessentityid
        , personinfo.PersonName
        from customerinfo
        left join personinfo on customerinfo.personid = personinfo.businessentityid
    )
select * from customerfinal