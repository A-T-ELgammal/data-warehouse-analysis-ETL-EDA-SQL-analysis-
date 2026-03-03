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

SELECT category, COUNT(product_key) AS total_products
FROM gold_layer.dim_product_info
WHERE category IS NOT NULL
GROUP BY category;

SELECT category, ROUND(AVG(product_cost), 2) AS average_price
FROM gold_layer.dim_product_info
WHERE category IS NOT NULL
GROUP BY category;

-- total revenue by customers 
SELECT 
    ci.customer_key,
    ci.first_name,
    ci.last_name,
    SUM(sls.total_sales) AS total_revenue
FROM gold_layer.fact_sales_info AS sls
LEFT JOIN gold_layer.dim_customer_info ci
ON sls.customer_key = ci.customer_key
GROUP BY
    ci.first_name,
    ci.last_name,
    ci.customer_key
ORDER BY total_revenue DESC;    

-- sold items across the countries 

SELECT
    ci.customer_country AS country,
    COUNT(sls.product_key) AS total_products
FROM gold_layer.fact_sales_info AS sls
LEFT JOIN gold_layer.dim_customer_info AS ci
ON sls.customer_key = ci.customer_key
GROUP BY country
ORDER BY total_products DESC;

-----------------------------------------------------------------------

--------- ranking analysis-------------------------

-- top 5 prouducts with highest revenue
SELECT
    pi.product_name,
    SUM(sls.total_sales) AS total_revenue
    , ROW_NUMBER() OVER (ORDER BY SUM(sls.total_sales) DESC) AS product_ranking

FROM gold_layer.fact_sales_info AS sls
LEFT JOIN gold_layer.dim_product_info AS pi 
ON sls.product_key = pi.product_key
GROUP BY pi.product_name
ORDER BY total_revenue DESC
-- for the worst- performing products 
-- ORDER BY total_revenue ASC
LIMIT 5;

-- top 3 customers with fewest and most orders placed 
SELECT
    ci.first_name,
    ci.last_name,
    sls.customer_key,
    COUNT(DISTINCT sls.order_number) AS total_orders
    -- ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT sls.order_number) ASC) AS ordered_placed_ranking
FROM gold_layer.fact_sales_info AS sls
LEFT JOIN gold_layer.dim_customer_info AS ci
ON sls.customer_key = ci.customer_key
GROUP BY 
    ci.first_name,
    ci.last_name,
    sls.customer_key
ORDER BY total_orders, first_name ASC -- fewest
-- ORDER BY total_orders DESC -- most
LIMIT 3;

========================================================================================

-- trends (measures) by date of dimensions
-- sales over time:
SELECT 
    order_date,
    total_sales
FROM gold_layer.fact_sales_info
WHERE total_sales IS NOT NULL
ORDER BY order_date

-- by year - total sales, total customers, total quantities
SELECT 
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT (MONTH FROM order_date) AS order_month,
    COUNT(DISTINCT customer_key) AS customers,
    SUM(quantity) AS total_quantities,
    SUM (total_sales) AS total_sales
FROM gold_layer.fact_sales_info
WHERE total_sales IS NOT NULL
GROUP BY order_year, order_month
ORDER BY order_year, order_month

--------------------------
-- cummulative analysis 
-- sales by month accumlatively 

SELECT 
    order_month,
    total_sales,
    SUM (total_sales) OVER (ORDER BY order_month) AS running_total_sales
FROM 
    (SELECT 
        EXTRACT(MONTH FROM order_date) AS order_month,
        SUM(total_sales) AS total_sales
    FROM gold_layer.fact_sales_info
    WHERE total_sales IS NOT NULL AND order_date IS NOT NULL
    GROUP BY order_month
    ORDER BY order_month
    ) AS monthly_total_sales


-----------------------------------

-- performancce analysis 
-- year-over-year-analysis
WITH yearly_product_sales AS 
(SELECT
    EXTRACT(YEAR FROM sls.order_date) AS order_year,
    pi.product_name,
    SUM (sls.total_sales) AS current_sales,
    ROUND(AVG(SUM(sls.total_sales)) OVER (PARTITION BY product_name), 2) AS avg_sales
FROM gold_layer.fact_sales_info AS sls
LEFT JOIN gold_layer.dim_product_info AS pi
ON sls.product_key = pi.product_key 
WHERE sls.order_date IS NOT NULL
GROUP BY order_year, pi.product_name)

SELECT 
    order_year,
    product_name,
    current_sales,
    CASE WHEN (current_sales - avg_sales) > 0 THEN 'ABOVE AVERAGE'
         WHEN (current_sales - avg_sales) < 0 THEN 'BELOW AVERAGE'
    END AS AVG_change,
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_year_sales,
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_sales_yearly,
    CASE    WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
            WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
            ELSE 'No changes'
    END AS previous_year_sales_status
FROM yearly_product_sales
-- WHERE previous_year_sales IS NOT NULL 
ORDER BY product_name, order_year

-- part to whole anlysis
-- sales percentage per category
WITH category_sales As
(SELECT
    pi.category,
    SUM(sls.total_sales) AS t_sales
FROM gold_layer.fact_sales_info AS sls
LEFT JOIN gold_layer.dim_product_info AS pi
ON sls.product_key = pi.product_key
GROUP BY pi.category
)

SELECT
category,
t_sales,
SUM(t_sales) OVER () AS overall_sales,
CONCAT(ROUND(((t_sales / SUM(t_sales) OVER () ) * 100), 2), '%') AS sales_percentage
FROM category_sales
ORDER BY sales_percentage DESC 

--- data segmentation ---

-- segment the cost range with the number of products
WITH cost_segment AS 
(SELECT
    product_key,
    product_name,
    product_cost,
    CASE 
        WHEN product_cost < 100 THEN 'BELOW 100'
        WHEN product_cost BETWEEN 100 AND 500 THEN '100-500'
        WHEN product_cost BETWEEN 500 AND 1000 THEN '100-500'
        WHEN product_cost > 1000 THEN 'ABOVE 1000'
    END AS  cost_range
        
FROM gold_layer.dim_product_info
)
 SELECT 
    cost_range,
    COUNT (product_key) AS total_products
FROM cost_segment
GROUP BY cost_range

-- segment customers for spending money 
WITH spending_per_customer AS
(
SELECT
    ci.customer_key,
    -- MIN(sls.order_date) AS first_order,
    -- MAX(sls.order_date) AS last_order,
    EXTRACT(YEAR FROM AGE(MAX(sls.order_date), MIN(sls.order_date))) * 12 + 
    EXTRACT(MONTH FROM AGE(MAX(sls.order_date), MIN(sls.order_date))) AS date_difference,
    SUM(sls.total_sales) AS total_spendings 
FROM gold_layer.fact_sales_info AS sls
LEFT JOIN gold_layer.dim_customer_info AS ci
ON sls.customer_key = ci.customer_key
GROUP BY 
        ci.customer_key
        )

SELECT
    customer_category,
    COUNT(customer_key) AS number_of_customer
FROM
    (SELECT 
        customer_key,
        total_spendings,
        date_difference,
        CASE 
            WHEN date_difference >= 12 AND total_spendings > 5000 THEN 'VIP'
            WHEN date_difference >= 12 AND total_spendings <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_category
    FROM spending_per_customer
    ) AS customer_segment
GROUP BY customer_category
