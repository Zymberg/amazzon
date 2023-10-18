with ad_budget as (

    select 
        category,
        Y_2017,
        Y_2018,
        Y_2019,
        Y_2020,
        Y_2021,
        Y_2022,
        Y_2023

    from {{ source('amazzon', 'ad_budget')}}
)

select * from ad_budget