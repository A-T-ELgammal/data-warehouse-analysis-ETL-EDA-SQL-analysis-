--------------------->>>> Silver layer <<<<<<<---------------------------
--> checking without result queries
SELECT COUNT(* )
FROM silver_layer.crm_product_info

-- check if primary key is redundant or not
SELECT product_id , COUNT(*)
FROM silver_layer.crm_product_info
GROUP BY product_id
HAVING COUNT(*) > 1 OR product_id IS NULL;
--
-- discover how to join using substring product_key
SELECT category_id FROM bronze_layer.erb_category_glv2;
SELECT 
-- check for joint matching in product category id 
SELECT 
product_key,
FROM bronze_layer.crm_product_info
-- WHERE REPLACE(SUBSTRING(product_key, 1,5),'-','_')  NOT IN (SELECT category_id FROM bronze_layer.erb_category_glv2)
-- WHERE REPLACE(SUBSTRING(product_key, 7, LENGTH(product_key)), '_', '-') NOT IN (SELECT sales_product_key FROM bronze_layer.crm_sales_info);

-----------

SELECT COUNT(product_name)
FROM bronze_layer.crm_product_info
WHERE product_name != TRIM(product_name)

SELECT product_cost
FROM bronze_layer.crm_product_info
WHERE  product_cost IS NULL;
--replace null with zero in product_cost
SELECT COALESCE(product_cost, 0)
FROM bronze_layer.crm_product_info

SELECT DISTINCT product_line FROM bronze_layer.crm_product_info

SELECT COUNT(product_start_date < product_end_date) FROM bronze_layer.crm_product_info

-- LEAD() to correct ate logic 
SELECT product_start_date, product_end_date,
LEAD(product_start_date) OVER (PARTITION BY product_key ORDER BY product_end_date)-1 AS product_end_date
FROM bronze_layer.crm_product_info
WHERE product_id IN (215, 216, 217, 218)
-------------------------------------------------------------------
---->> crm sales table --- testin and data quality
SELECT * FROM silver_layer.crm_sales_info

SELECT sales_order_number, sales_product_key, sales_customer_id
FROM silver_layer.crm_sales_info
WHERE sales_order_number != TRIM(sales_order_number)
    OR sales_product_key != TRIM(sales_product_key)

select * from silver_layer.crm_customer_info;

SELECT sales_order_number
FROM silver_layer.crm_sales_info
WHERE sales_product_key NOT IN (SELECT product_key FROM silver_layer.crm_product_info)

SELECT sales_customer_id
FROM silver_layer.crm_sales_info
WHERE sales_customer_id NOT IN (SELECT customer_id FROM silver_layer.crm_customer_info)
-- check for invalid date

SELECT sales_order_date,
CASE   WHEN sales_order_date = 0 THEN NULL
       WHEN LENGTH(CAST(sales_order_date AS VARCHAR)) != 8 THEN NULL
       ELSE CAST(CAST(sales_order_date AS VARCHAR) AS DATE)
       END AS sales_order_date
       FROM silver_layer.crm_sales_info

SELECT sales_shipping_date,
CASE   WHEN sales_shipping_date = 0 THEN NULL
       WHEN LENGTH(CAST(sales_shipping_date AS VARCHAR)) != 8 THEN NULL
       ELSE CAST(CAST(sales_shipping_date AS VARCHAR) AS DATE)
       END AS sales_shipping_date
       FROM silver_layer.crm_sales_info


SELECT sales_order_date, sales_due_date
FROM silver_layer.crm_sales_info
WHERE sales_order_date > sales_due_date OR sales_order_date > sales_shipping_date

SELECT customer_key FROM bronze_layer.crm_customer_info;
SELECT customer_id FROM bronze_layer.erb_customer_az12;

----------------------------------------------------------------
-- erb_customer_az12
 SELECT * FROM bronze_layer.erb_customer_az12

 -- customer_id
 SELECT 
 CASE   WHEN customer_id ~ '^NAS' THEN SUBSTRING((TRIM(customer_id)),4, LENGTH((TRIM(customer_id))))
 END AS customer_id
 FROM bronze_layer.erb_customer_az12
 WHERE CASE   WHEN customer_id ~ '^NAS' THEN SUBSTRING((TRIM(customer_id)),4, LENGTH((TRIM(customer_id))))
 END AS customer_id NOT IN ((SELECT customer_key FROM bronze_layer.crm_customer_info))
 
  -- check for join with crm_customer_info
SELECT customer_id FROM bronze_layer.erb_customer_az12
WHERE( CASE   WHEN customer_id ~ '^NAS' THEN SUBSTRING((TRIM(customer_id)),4, LENGTH((TRIM(customer_id))))
 END ) NOT IN ((SELECT customer_key FROM bronze_layer.crm_customer_info))

 -- birth_date
SELECT birth_date
FROM bronze_layer.erb_customer_az12
ORDER BY birth_date DESC
WHERE birth_date > CURRENT_DATE;

-- gender 
SELECT DISTINCT gender
FROM bronze_layer.erb_customer_az12
WHERE TRIM(gender) != gender

SELECT DISTINCT
CASE WHEN TRIM(gender) = 'M' THEN 'Male'
    WHEN TRIM (gender) = 'F' THEN 'Female'
    ELSE TRIM(gender)
END AS gender
FROM bronze_layer.erb_customer_az12
 ------------------------------------------------------------
 -- erb_location_az12
