{{  config(materialized='table')  }}
with country_region as (
    select *
    from {{ref('stg_country_region')}} 
    )
    , stateprovince as (
        select  *
        from {{ ref('stg_stateprovince') }} 
    )
    , addresss as (
        select *
        from {{ref('stg_address')}} 
    )
    , final as (
        select 
            row_number() over (order by addressid) as addressSK
            , addresss.addressid as addressid
            , addresss.city as city
            , addresss.postalcode as postalcode
            , addresss.addressline1 as addressline1
            , addresss.addressline2 as addressline2
            , stateprovince.name as stateprovince_name
            , country_region.name as country_region_name
            , addresss.spatiallocation as spatiallocation
        from addresss
        left join stateprovince on addresss.stateprovinceid = stateprovince.stateprovinceid
        left join country_region on stateprovince.countryregioncode = country_region.countryregioncode
    )

select * from final