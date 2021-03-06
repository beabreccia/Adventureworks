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
        customer.customerSK as customer_fk
        , customer.PersonName
        , customer.customerid
        , orderheader.salesorderid
        , orderheader.billtoaddressid as address_fk
        , orderheader.creditcardid as creditcard_fk
        , orderheader.orderdate
        , salesreason.name_motivo
        
        --, salesreason.reasontype
        --, salesreason.salesreasonSK
        , case when orderheader.status = 1 then 'In Process'
                when orderheader.status = 2 then 'Approved'
                when orderheader.status = 3 then 'Waiting'
                when orderheader.status = 4 then 'Rejected'
                when orderheader.status = 5 then 'Shipped'
                when orderheader.status = 6 then 'Cancelled'
        end as Status
        from {{ ref('stg_salesorderheader') }} as orderheader
        left join dim_salesreason salesreason on orderheader.salesorderid = salesreason.salesorderid
        left join dim_customer customer on orderheader.customerid = customer.customerid
    )
    , salesorderdetail_sk as (
        select
         product.produtoSK as product_fk
        , product.produto
        , orderdetail.salesorderid
        , orderdetail.salesorderdetailid
        , orderdetail.orderqty			
        , orderdetail.unitprice	
        , orderdetail.unitpricediscount
        from {{ ref('stg_salesordersdetail') }} as orderdetail
        left join dim_products product on orderdetail.productid = product.produtoid
    ) 
, final as (
    select 
    salesorder_sk.customer_fk
        , salesorder_sk.address_fk
        , salesorder_sk.PersonName
        , salesorder_sk.customerid
        --, salesorder_sk.salesreasonSK
        , salesorder_sk.orderdate
        , salesorder_sk.name_motivo
        --, salesorder_sk.reasontype
        , extract(year from salesorder_sk.orderdate) as OrderYear
        , extract(month from salesorder_sk.orderdate) as OrderMonth
        , salesorder_sk.Status
        , salesorderdetail_sk.salesorderdetailid
        , salesorderdetail_sk.product_fk
        , salesorderdetail_sk.produto
        , salesorderdetail_sk.orderqty			
        , salesorderdetail_sk.unitprice	
        , salesorderdetail_sk.unitpricediscount
        , dim_addres.city
        , dim_addres.stateprovince_name
        , dim_addres.country_region_name
        , dim_creditcard.creditcardSK
        , dim_creditcard.cardtype as CardType
        , salesorder_sk.salesorderid
    from salesorder_sk
    left join  salesorderdetail_sk on salesorder_sk.salesorderid = salesorderdetail_sk.salesorderid
    left join dim_addres on salesorder_sk.address_fk = dim_addres.addressSK
    left join dim_creditcard on salesorder_sk.creditcard_fk = dim_creditcard.creditcardid
)

, final2 as (
    select unitprice*orderqty as GrossIncome
    , *
    from final
)

SELECT * FROM final2
/*
select customer_fk, salesorderid as reason_fk, salesorderdetailid, product_fk, address_fk, creditcardSK
, orderdate, OrderYear, OrderMonth, Status, orderqty, unitprice, GrossIncome, unitpricediscount
from final2
order by salesorderdetailid*/