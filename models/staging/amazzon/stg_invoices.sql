with invoices as (
    
    select
        invoice_id,
        date,
        market,
        product_id,
        units,
        sales

    from {{ source('amazzon', 'invoices')}}
)

select * from invoices