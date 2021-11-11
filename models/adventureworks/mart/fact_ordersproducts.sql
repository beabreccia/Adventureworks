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
    , salesorderheader_sk as (
        select
            salesorderheader.salesorderid as salesorderid
            , dim_salesorderheader.salesorderheaderSK as salesorderheaderSK
            , salesorderheader.freight as freight
        from {{  ref('stg_salesorderheader')  }} salesorderheader
        left join dim_salesorderheader on dim_salesorderheader.salesorderid = salesorderheader.salesorderid
    )
    , salesorderdetail_sk as (
        select
            salesorderdetail.salesorderid as salesorderid
            , salesorderdetail.salesorderdetailid as salesorderdetailid
            , dim_products.produtoSK as produtoSK
            , salesorderdetail.unitprice as unitprice
            , salesorderdetail.orderqty as orderqty
            , salesorderdetail.unitprice*salesorderdetail.orderqty*(1-salesorderdetail.unitpricediscount) as valuen
        
        from {{  ref('stg_salesordersdetail')  }} salesorderdetail
        left join dim_products on dim_products.produtoid = salesorderdetail.productid
    )
    , final as (
        select
            salesorderheader_sk.salesorderheaderSK
            , salesorderdetail_sk.produtoSK
            , salesorderdetail_sk.unitprice
            , salesorderdetail_sk.orderqty
            , salesorderdetail_sk.valuen
        from salesorderheader_sk
        left join salesorderdetail_sk on salesorderheader_sk.salesorderid = salesorderdetail_sk.salesorderid
    )

select * from final