{{  config(materialized='table')  }}

with
    products as (
        select *
        from {{  ref('stg_products')  }}
    )
    ,category as (
        select *
        from {{  ref('stg_product_category')  }}
    )
    ,subcategory as (
        select *
        from {{  ref('stg_product_subcategory')  }}
    )
    , final as (
        select
            {{ dbt_utils.surrogate_key('productid') }} as produtoSK
            , products.productid as produtoid
            , products.name as produto
            , products.productnumber as productnumber
            , products.class as class
            , products.reorderpoint as reorderpoint
            , products.standardcost as standardcost
            , products.listprice as listprice
            , products.safetystocklevel as safetystocklevel
            , products.size as size
            , products.sizeunitmeasurecode as sizeunitmeasurecode
            , products.weight as weight
            , products.weightunitmeasurecode as weightunitmeasurecode
            , products.color as color
            , products.style as style
            , products.daystomanufacture as daystomanufacture
            , products.productline as productline
            , products.makeflag as makeflag
            , products.finishedgoodsflag as finishedgoodsflag
            , products.sellstartdate as sellstartdate
            , products.sellenddate as sellenddate
            , products.modifieddate as modifieddate
            , category.name as category_name
            , subcategory.name as subcategory_name
        from products
        left join subcategory on products.productsubcategoryid = subcategory.productsubcategoryid
        left join category on subcategory.productcategoryid = category.productcategoryid
    )

select * from final