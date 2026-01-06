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