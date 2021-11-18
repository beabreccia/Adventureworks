{{  config(materialized='table')  }}
with
    salesreason as (
        select *
        from {{  ref('stg_salesreason')  }}
    )
    , final as (
        select
            row_number() over (order by salesreasonid)  as salesreasonSK
            , salesreasonid as salesreasonid
            , name as name_motivo
            , reasontype as reasontype
        from salesreason
    )
select * from final
