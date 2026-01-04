/*
===============================================================================
Cumulative Analysis
===============================================================================
Purpose:
    - To calculate running totals or moving averages for key metrics.
    - To track performance over time cumulatively.
    - Useful for growth analysis or identifying long-term trends.

SQL Functions Used:
    - Window Functions: SUM() OVER(), AVG() OVER()
===============================================================================
*/

-- Calculate the total sales per month 
-- and the running total of sales over time 
SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (
    order by
      order_date
  ) AS running_total
FROM
  (
    SELECT
      date_format (order_date, '%Y-%m-01') AS order_date,
      SUM(sales_amount) AS total_sales
    FROM
      gold.fact_sales
    WHERE
      order_date is not null
    GROUP BY
      date_format (order_date, '%Y-%m-01')
    ORDER BY
      date_format (order_date, '%Y-%m-01')
  ) t;
