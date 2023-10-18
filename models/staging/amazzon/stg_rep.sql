with rep as (
    select
        product_id,
        country,
        rep

    from {{ source('amazzon', 'rep')}}

)

select * from rep