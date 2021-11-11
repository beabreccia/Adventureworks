{{  config(materialized='table')  }}

with
    salesorderheader as (
        select *
        from {{ ref('stg_salesorderheader')  }}
    )
    , person as (
        select *
        from {{  ref('stg_person')  }}
    )
    , customer as (
        select *
        from {{  ref('stg_customer')  }}
    )
    , addresss as (
        select *
        from {{  ref('stg_address')  }}
    )
    , stateprovince as (
        select *
        from {{  ref('stg_stateprovince')  }}
    )
    , country_region as (
        select *
        from {{  ref('stg_country_region')  }}
    )
    , creditcard as (
        select *
        from {{  ref('stg_creditcard')  }}
    )
    , final as (
        select
            {{ dbt_utils.surrogate_key('salesorderheader.salesorderid') }} as salesorderheaderSK
            , salesorderheader.salesorderid as salesorderid
            , salesorderheader.subtotal as subtotal
            , case
                when salesorderheader.status = 1 then 'Em Processamento'
                when salesorderheader.status = 2 then 'Aprovado'
                when salesorderheader.status = 3 then 'Em Espera'
                when salesorderheader.status = 4 then 'Rejeitado'
                when salesorderheader.status = 5 then 'Enviado'
                when salesorderheader.status = 6 then 'Cancelado'
              end as status
            , person.title as customer
            , person.firstname as firstname
            , person.lastname as lastname
            , salesorderheader.orderdate as data
            , extract(year from salesorderheader.orderdate) as year_date
            , extract(month from salesorderheader.orderdate) as month_date
            , country_region.name as country_region_name
            , stateprovince.name as stateprovince_name
            , addresss.city as city
            , creditcard.cardtype as cardtype

        from salesorderheader
        left join customer on customer.customerid = salesorderheader.customerid
        left join person on person.businessentityid = customer.personid
        left join addresss on addresss.addressid = salesorderheader.shiptoaddressid
        left join stateprovince on stateprovince.stateprovinceid = addresss.stateprovinceid
        left join country_region on country_region.countryregioncode = stateprovince.countryregioncode
        left join creditcard on creditcard.creditcardid = salesorderheader.creditcardid
    )

SELECT distinct * from final