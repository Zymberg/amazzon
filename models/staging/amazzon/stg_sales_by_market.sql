-- Replace 'your_market_sales' with an appropriate name for the model
WITH ProductMarketSales AS (

    SELECT
        p.Product_Name,
        s.Market,
        SUM(s.Sales) AS TotalSales

    FROM {{ source('amazzon', 'invoices')}} AS s 
    JOIN  {{ source('amazzon', 'products')}}
 AS p
    ON s.Product_ID = p.Product_ID
    GROUP BY p.Product_Name, s.Market
),
RankedSales AS (
    SELECT
        Product_Name,
        Market,
        TotalSales,
        ROW_NUMBER() OVER (PARTITION BY Product_Name ORDER BY TotalSales DESC) AS MarketRank
    FROM ProductMarketSales
),

final as (
SELECT
    Product_Name,
    Market,
    TotalSales
FROM RankedSales
WHERE MarketRank = 1
)

SELECT * from final