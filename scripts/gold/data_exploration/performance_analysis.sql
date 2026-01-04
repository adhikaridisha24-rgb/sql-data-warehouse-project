/*
===============================================================================
Performance Analysis (Year-over-Year, Month-over-Month)
===============================================================================
Purpose:
    - To measure the performance of products, customers, or regions over time.
    - For benchmarking and identifying high-performing entities.
    - To track yearly trends and growth.

SQL Functions Used:
    - LAG(): Accesses data from previous rows.
    - AVG() OVER(): Computes average values within partitions.
    - CASE: Defines conditional logic for trend analysis.
===============================================================================
*/

/* Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales */
WITH
  yearly_product AS (
    SELECT
      YEAR (f.order_date) AS order_year,
      p.product_name,
      SUM(f.sales_amount) AS current_sales
    FROM
      gold.fact_sales f
      LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    GROUP BY
      p.product_name,
      YEAR (f.order_date)
  )
SELECT
  order_year,
  product_name,
  current_sales,
  AVG(current_sales) OVER (
    PARTITION BY
      product_name
  ) AS avg_sales,
  current_Sales - AVG(current_sales) OVER (
    PARTITION BY
      product_name
  ) AS avg_diff,
  CASE
    WHEN current_Sales - AVG(current_sales) OVER (
      PARTITION BY
        product_name
    ) > 0 THEN 'Above avg'
    WHEN current_Sales - AVG(current_sales) OVER (
      PARTITION BY
        product_name
    ) < 0 THEN 'Below avg'
    ELSE 'Average'
  END AS avg_change,
  lag (current_sales) OVER (
    PARTITION BY
      product_name
    ORDER BY
      order_year
  ) AS prev_sales,
  current_sales - lag (current_sales) OVER (
    PARTITION BY
      product_name
    ORDER BY
      order_year
  ) AS prev_sales_diff,
  CASE
    WHEN current_Sales - lag (current_sales) OVER (
      PARTITION BY
        product_name
      ORDER BY
        order_year
    ) > 0 THEN 'Increase'
    WHEN current_Sales - lag (current_sales) OVER (
      PARTITION BY
        product_name
      ORDER BY
        order_year
    ) < 0 THEN 'Decrease'
    ELSE 'No change'
  END AS prev_sales_change
FROM
  yearly_product
ORDER BY
  product_name;
