-- Dimension exploration 

-- customers & products 

SELECT DISTINCT customer_country
FROM gold_layer.dim_customer_info;

SELECT DISTINCT category, sub_category
FROM gold_layer.dim_product_info
ORDER BY 1,2;

SELECT DISTINCT product_line
FROM gold_layer.dim_product_info;

-------------------------------
-- Date exploration

SELECT 
MIN(order_date) AS first_order_date,
MAX(order_date) AS last_order_date
FROM gold_layer.fact_sales_info;

SELECT
AGE(MAX(order_date), MIN(order_date)) AS orders_full_date_range
FROM gold_layer.fact_sales_info;


SELECT
MIN(customer_birth_date) AS younger_customer,
MAX(customer_birth_date) AS oldest_customer,
AGE(MAX(customer_birth_date), MIN(customer_birth_date)) AS customer_age_range,
AGE(NOW()::date, MIN(customer_birth_date)) AS youngest_customer_age,
AGE(NOW()::date, MAX(customer_birth_date)) AS oldest_customer_age
FROM gold_layer.dim_customer_info

-------------------------------------------------------
-- measure exploration

SELECT 'total_orders', COUNT(DISTINCT order_number)
FROM gold_layer.fact_sales_info
UNION ALL

SELECT 'total store customers', COUNT(DISTINCT customer_id)
FROM gold_layer.dim_customer_info
UNION ALL 

SELECT 'total customer who placed orders', COUNT(DISTINCT customer_key)
FROM gold_layer.fact_sales_info
UNION ALL

SELECT 'total store products', COUNT(DISTINCT product_key)
FROM gold_layer.fact_sales_info
UNION ALL

SELECT 'sum of total sales', SUM(total_sales)
FROM gold_layer.fact_sales_info
UNION ALL

SELECT 'average sales', ROUND(AVG(total_sales), 2)
FROM gold_layer.fact_sales_info
UNION ALL

SELECT 'total sold products', SUM(quantity)
FROM gold_layer.fact_sales_info
UNION ALL

SELECT 'product average price', ROUND(AvG(price), 2) 
FROM gold_layer.fact_sales_info

----------------------------------------------
-- magnitude analysis -- (insights from data)

SELECT 
    customer_country, COUNT(8) AS total_customers
FROM gold_layer.dim_customer_info
GROUP BY customer_country
ORDER BY customer_country ASC;

SELECT customer_gender, COUNT(customer_key) AS total_customers 
FROM gold_layer.dim_customer_info
GROUP BY customer_gender;
