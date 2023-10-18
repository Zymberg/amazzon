WITH CategoryYear AS (
    SELECT
        A.Category,
        Y.Year
    FROM (
        SELECT 
            DISTINCT Category 
        
        FROM {{ ref('stg_ad_budget') }}
    ) AS A
    CROSS JOIN (
        SELECT 
            DISTINCT LEFT(Date, 4) AS Year 
        
        FROM {{ ref('stg_invoices') }}
    ) AS Y
),
Budget AS (
    SELECT
        CY.Category,
        CY.Year,
        CASE
            WHEN CY.Year = '2017' THEN Y_2017
            WHEN CY.Year = '2018' THEN Y_2018
            WHEN CY.Year = '2019' THEN Y_2019
            WHEN CY.Year = '2020' THEN Y_2020
            WHEN CY.Year = '2021' THEN Y_2021
            WHEN CY.Year = '2022' THEN Y_2022
            WHEN CY.Year = '2023' THEN Y_2023
        END AS Budget
    FROM CategoryYear AS CY
    LEFT JOIN {{ ref('stg_ad_budget') }} AS B ON CY.Category = B.Category
),
Sales AS (
    SELECT
        LEFT(Date, 4) AS Year,
        SUM(Sales) AS Total_Sales
    FROM {{ ref('stg_invoices') }}
    GROUP BY Year
),
Combined AS (
    SELECT
        CY.Category,
        CY.Year,
        COALESCE(S.Total_Sales, 0) AS Total_Sales,
        COALESCE(B.Budget, 0) AS Budget
    FROM CategoryYear AS CY
    LEFT JOIN Sales AS S ON CY.Year = S.Year
    LEFT JOIN Budget AS B ON CY.Category = B.Category AND CY.Year = B.Year
),
Final AS (
    SELECT
        Category,
        Year,
        Total_Sales,
        Budget,
        CASE
          WHEN Budget = 0 THEN 0
          ELSE (Total_Sales / Budget) * 100 -- Convert to percentage
        END AS SuccessRate
    FROM Combined
    ORDER BY Category, Year
)
-- Create a table with the results

    SELECT * FROM Final
