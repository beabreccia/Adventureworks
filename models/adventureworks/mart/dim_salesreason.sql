{{  config(materialized='table')  }}

with
    salesreason as (
        select *
        from {{  ref('stg_salesreason')  }}
    )
    , final as (
        select
            {{ dbt_utils.surrogate_key('salesreasonid') }} as salesreasonSK
            , salesreasonid as salesreasonid
            , name as name_motivo
            , reasontype as reasontype
        from salesreason
    )

select * from final