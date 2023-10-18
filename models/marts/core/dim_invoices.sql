with invoices as (
    
    select
        invoice_id,
        date,
        market,
        product_id,
        units,
        sales
    from {{ ref('stg_invoices')}}
),

products as (
    select
        product_id,
        product_name,
        brand
    from {{ ref('stg_products')}}
),

 
rep as (
    select
        product_id,
        country,
        rep
    from {{ ref('stg_rep')}}
),

MaxUnitsSales as (
    select
        i.Invoice_ID,
        max(i.Units) as Max_Units,
        max(i.Sales) as Max_Sales
    from invoices i
    where i.Invoice_ID is not null
    group by i.Invoice_ID
),


final as (
    select
        i.invoice_id,
        i.date,
        i.market,
        i.product_id,
        p.product_name,
        p.brand,
        i.units,
        i.sales,
        r.rep
  FROM invoices i
JOIN products p ON i.Product_ID = p.Product_ID
JOIN rep r ON i.Product_ID = r.Product_ID AND i.Market = r.Country
JOIN MaxUnitsSales mu ON i.Invoice_ID = mu.Invoice_ID
WHERE i.Invoice_ID IS NOT NULL
    AND i.Units = mu.Max_Units
    AND i.Sales = mu.Max_Sales
)

select * from final