with sales_fr as (
    
    SELECT
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

    from {{ref('stg_sales_fr')}}

),

products as (
    select
        product_id,
        product_name,
        brand

    from {{ ref('stg_products')}}

),

final as (

    SELECT
    S.Product_ID,
    P.Product_Name,
    TO_VARIANT(S.Year) AS Year,
    SUM(S.January) AS January_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.January ELSE 0 END) AS January_Sales,
    SUM(S.February) AS February_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.February ELSE 0 END) AS February_Sales,
    SUM(S.March) AS March_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.March ELSE 0 END) AS March_Sales,
    SUM(S.April) AS April_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.April ELSE 0 END) AS April_Sales,
    SUM(S.May) AS May_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.May ELSE 0 END) AS May_Sales,
    SUM(S.June) AS June_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.June ELSE 0 END) AS June_Sales,
    SUM(S.July) AS July_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.July ELSE 0 END) AS July_Sales,
    SUM(S.August) AS August_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.August ELSE 0 END) AS August_Sales,
    SUM(S.September) AS September_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.September ELSE 0 END) AS September_Sales,
    SUM(S.October) AS October_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.October ELSE 0 END) AS October_Sales,
    SUM(S.November) AS November_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.November ELSE 0 END) AS November_Sales,
    SUM(S.December) AS December_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.December ELSE 0 END) AS December_Sales,
    SUM(S.January + S.February + S.March + S.April + S.May + S.June + S.July + S.August + S.September + S.October + S.November + S.December) AS Total_Units,
    SUM(CASE WHEN S.KPI = 'Units' THEN S.January + S.February + S.March + S.April + S.May + S.June + S.July + S.August + S.September + S.October + S.November + S.December ELSE 0 END) AS Total_Sales
FROM sales_fr S
JOIN products P ON S.Product_ID = P.Product_ID
WHERE S.Year = 2022
GROUP BY S.Product_ID, P.Product_Name, S.Year

)

select * from final