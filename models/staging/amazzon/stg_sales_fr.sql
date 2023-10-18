with sales_fr as (
    select
        id_invoice,
        market,
        product_id,
        kpi,
        year,
        january,
        february,
        march,
        april,
        may,
        june,
        july,
        august,
        September,
        October,
        November,
        December

    from {{ source('amazzon', 'sales_fr')}}
)

select * from sales_fr