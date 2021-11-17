{{  config(materialized='table')  }}
with
    dim_salesorderheader as (
        select *
        from {{ ref('dim_salesorderheader')  }}
    )
    , dim_products as (
        select *
        from {{ ref('dim_products')  }}
    )
    , dim_addres as (
        select *
        from {{ ref('dim_address')  }}
    )
    , dim_creditcard as (
        select *
        from {{ ref('dim_creditcard') }}
    )
    , dim_customer as (
        select *
        from {{ ref('dim_customer') }}
    )
    , dim_salesreason as (
        select *
        from {{ ref('dim_salesreason') }}
    )
    , salesorder_sk as (
    select 
        customer.customer_pk as customer_fk
        , customer.CustomerName
        , orderheader.salesorderid
        , orderheader.billtoaddressid as address_fk
        , orderheader.creditcardid as creditcard_fk
        , orderheader.orderdate
        , salesreason.Reasons
        , case
        when orderheader.status = 1 then 'In Process'
        when orderheader.status = 2 then 'Approved'
        when orderheader.status = 3 then 'Waiting'
        when orderheader.status = 4 then 'Rejected'
        when orderheader.status = 5 then 'Shipped'
        when orderheader.status = 6 then 'Cancelled'
        end as Status
        from {{ ref('stg_salesorderheader') }} as orderheader
        left join dim_salesreason salesreason on orderheader.salesorderid = salesreason.salesorderid
        left join dim_customer customer on orderheader.customerid = customer.customer_pk
    )
    , salesorderdetail_sk as (
        select
         product.product_pk as product_fk
        , product.ProductName
        , orderdetail.salesorderid
        , orderdetail.salesorderdetailid
        , orderdetail.orderqty			
        , orderdetail.unitprice	
        , orderdetail.unitpricediscount
        from {{ ref('stg_salesorderdetail') }} as orderdetail
        left join dim_products product on orderdetail.productid = product.product_pk
    )
, final as (
    select salesorder_sk.customer_fk
        , salesorder_sk.address_fk
        , salesorder_sk.customername
        , salesorder_sk.salesorderid
        , salesorder_sk.orderdate
        , extract(year from salesorder_sk.orderdate) as OrderYear
        , extract(month from salesorder_sk.orderdate) as OrderMonth
        , salesorder_sk.Status
        , salesorderdetail_sk.salesorderdetailid
        , salesorderdetail_sk.product_fk
        , salesorderdetail_sk.productname
        , salesorderdetail_sk.orderqty			
        , salesorderdetail_sk.unitprice	
        , salesorderdetail_sk.unitpricediscount
        , addressinfo.CityName
        , addressinfo.StateName
        , addressinfo.CountryName
        , creditcard.creditcard_pk as creditcard_fk
        , creditcard.cardtype as CardType
    from salesorder_sk
    left join  salesorderdetail_sk salesorderdetail_sk  on salesorder_sk.salesorderid = salesorderdetail_sk.salesorderid
    left join addressinfo on salesorder_sk.address_fk = addressinfo.address_pk
    left join creditcard on salesorder_sk.creditcard_fk = creditcard.creditcard_pk
)
, final2 as (
    select unitprice*orderqty as GrossIncome
    , *
    from final
)