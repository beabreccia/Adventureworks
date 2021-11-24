{{  config(materialized='table')  }}

with stgdate as (
    select
        distinct 
        orderdate as date
        
    from {{ ref('stg_salesorderheader') }}
),
finaldate as (
    select 
        row_number() over (order by date) as date_sk
        , *
    from stgdate
)
select * from finaldate