{{  config(materialized='table')  }}
with
    salesreason as (
        select *
        from {{  ref('stg_salesreason')  }}
    )
    , salesorderheadersalesreason as (
        select salesorderid
            , salesreasonid
	    from {{ ref('stg_salesordesrheadersalesreason') }}
    )
    , reasonalfinal as (
        select salesorderreason.salesorderid
        , salesorderreason.salesreasonid
        , salesreason.reasontype
        , salesreason.name 
        from salesorderheadersalesreason salesorderreason
        left join salesreason on salesreason.salesreasonid = salesorderreason.salesreasonid
        where salesorderreason.salesorderid is not null
    )
    , final as (
        select
            row_number() over (order by salesreasonid)  as salesreasonSK
            , salesorderid as salesorderid
            , salesreasonid as salesreasonid
            , name as name_motivo
            , reasontype as reasontype
        from reasonalfinal
    )
select * from final
