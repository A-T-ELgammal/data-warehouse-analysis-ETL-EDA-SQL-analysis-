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

SELECT * FROM gold_layer.fact_sales_info

SELECT
COUNT(DISTINCT order_number) AS total_orders,
COUNT(DISTINCT customer_key) AS total_customer_place,
COUNT(DISTINCT product_key) AS total_store_products,
SUM(total_sales) AS sum_of_total_sales,
ROUND(AVG(total_sales), 2) AS average_sales,
SUM(quantity) AS total_sold_products,
ROUND(AvG(price), 2) AS product_average_price
FROM gold_layer.fact_sales_info
