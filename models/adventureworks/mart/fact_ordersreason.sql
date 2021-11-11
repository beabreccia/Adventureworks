{{  config(materialized='table')  }}
with
    dim_salesorderheader as (
        select *
        from {{ ref('dim_salesorderheader')  }}
    )
    , dim_salesreason as (
        select *
        from {{ ref('dim_salesreason')  }}
    )
    , salesorderheader_sk as (
        select
            salesorderheader.salesorderid as salesorderid
            , dim_salesorderheader.salesorderheaderSK as salesorderheaderSK
        from {{  ref('stg_salesorderheader')  }} salesorderheader
        left join dim_salesorderheader on dim_salesorderheader.salesorderid = salesorderheader.salesorderid
    )
    , salesreason_sk as (
        select
            salesordersheadersalesreason.salesorderid as salesorderid
            , salesordersheadersalesreason.salesreasonid as salesreasonid
            , dim_salesreason.salesreasonSK as salesreasonSK
        
        from {{  ref('stg_salesordesrheadersalesreason')  }} salesordersheadersalesreason
        left join dim_salesreason on dim_salesreason.salesreasonid = salesordersheadersalesreason.salesreasonid
    )
    , final as (
        select
            salesorderheader_sk.salesorderheaderSK
            , salesreason_sk.salesreasonSK
        from salesorderheader_sk
        left join salesreason_sk on salesorderheader_sk.salesorderid = salesreason_sk.salesorderid
    )

select * from final