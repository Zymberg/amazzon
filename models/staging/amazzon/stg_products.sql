with products as (
    
    select
        product_id,
        product_name,
        brand

    from {{ source('amazzon', 'products')}}

)

select * from products
