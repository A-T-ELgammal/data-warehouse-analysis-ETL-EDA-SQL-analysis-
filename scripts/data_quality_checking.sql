
--> cleaning the crm_customer_info table --- checking with no results

SELECT customer_id, COUNT(*) 
FROM silver_layer.crm_customer_info
GROUP BY customer_id
HAVING COUNT(*) > 1;


ALTER TABLE bronze_layer.crm_customer_info 
RENAME COLUMN customer_material_status TO customer_marital_status;

ALTER TABLE silver_layer.crm_customer_info 
RENAME COLUMN customer_material_status TO customer_marital_status;

SELECT DISTINCT customer_marital_status
FROM silver_layer.crm_customer_info

SELECT * 
FROM silver_layer.crm_customer_info
WHERE customer_id = 29466;
--> selecting just latest update of customer info 
SELECT * 
FROM(
SELECT *
, RANK() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC) AS updated_state
FROM bronze_layer.crm_customer_info
)AS ranked_customers WHERE updated_state = 1;

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
SELECT * FROM bronze_layer.erb_location_a101

SELECT REGEXP_REPLACE((TRIM(customer_id)), '-','') AS customer_id
FROM bronze_layer.erb_location_a101
WHERE REGEXP_REPLACE((TRIM(customer_id)), '-','') NOT IN (SELECT customer_id FROM silver_layer.erb_customer_az12)

SELECT DISTINCT country
FROM bronze_layer.erb_location_a101
-- WHERE TRIM(country) != country

CASE   WHEN country IN ('US', 'USA', 'United States') THEN 'USA'
       WHEN country = 'DE' THEN 'Germany'
       ELSE country
END AS country

FROM bronze_layer.erb_location_a101;

------------------------------------------
-- category glv2
SELECT * FROM bronze_layer.erb_category_glv2
--category_id
SELECT * 
FROM bronze_layer.erb_category_glv2
WHERE category_id NOT IN (SELECT category_id FROM silver_layer.crm_product_info)
--category
SELECT DISTINCT category
FROM bronze_layer.erb_category_glv2
-- WHERE TRIM(category) != category
--subcategory
SELECT DISTINCT sub_category
FROM bronze_layer.erb_category_glv2
--
-- maintainance 
SELECT DISTINCT maintenance
FROM bronze_layer.erb_category_glv2
---------------------------------------------
SELECT COUNT(DISTINCT country)
FROM silver_layer.erb_location_a101

-------------------------- Gold Layer ----------------------------------
-- check for duplicates after joining by master table primary key 

SELECT customer_id, COUNT(*)
FROM (SELECT 
crm_ci.customer_id,
crm_ci.customer_key,
crm_ci.customer_first_name,
crm_ci.customer_last_name,
crm_ci.customer_marital_status,
crm_ci.customer_create_date,
erb_caz12.birth_date,
erb_caz12.gender,
erb_loc.country
FROM silver_layer.crm_customer_info AS crm_ci
LEFT JOIN silver_layer.erb_customer_az12 AS erb_caz12 
ON crm_ci.customer_key = erb_caz12.customer_id
LEFT JOIN silver_layer.erb_location_a101 erb_loc
ON crm_ci.customer_key = erb_loc.customer_id
) AS temp
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- deleting 4 rows with all null in all fields excepts customer_key 
DELETE FROM silver_layer.crm_customer_info
WHERE customer_key IN ('A01Ass', 'PO25', '13451235', 'SF566')

select * FROM silver_layer.crm_customer_info

-- checking for null and difference in gender for joining crm_customer_info and erb_customer_az12

SELECT 
crm_ci.customer_gender,
erb_caz12.gender
-- COUNT(*)
FROM silver_layer.crm_customer_info AS crm_ci
LEFT JOIN silver_layer.erb_customer_az12 AS erb_caz12 
ON crm_ci.customer_key = erb_caz12.customer_id
LEFT JOIN silver_layer.erb_location_a101 erb_loc
ON crm_ci.customer_key = erb_loc.customer_id
ORDER BY 1,2
-- 4569 total unknown values in customer info

-- WHERE crm_ci.customer_gender != erb_caz12.gender
WHERE crm_ci.customer_gender = 'Unknown';

-- 1472 total null values in gender
SELECT COUNT(*) FROM silver_layer.erb_customer_az12
WHERE gender IS NULL

-- checking missing date after refinement of joininig data 
SELECT COUNT(*)
FROM
(SELECT 
crm_ci.customer_id,
crm_ci.customer_key,
crm_ci.customer_first_name,
crm_ci.customer_last_name,
crm_ci.customer_marital_status,
        -- trust crm customer gender if differrnce appears 
CASE    WHEN crm_ci.customer_gender != 'Unknown' AND erb_caz12.gender IS NOT NULL
        AND crm_ci.customer_gender != erb_caz12.gender THEN crm_ci.customer_gender
        -- trust crm customer gender if erb gender null
        WHEN erb_caz12.gender IS NULL AND crm_ci.customer_gender != 'Unknown'
        THEN crm_ci.customer_gender
        
        WHEN crm_ci.customer_gender = 'Unknown' AND erb_caz12.gender IS NOT NULL 
        THEN erb_caz12.gender

        ELSE crm_ci.customer_gender
END AS customer_gender,
crm_ci.customer_create_date,
erb_caz12.birth_date,
erb_loc.country
FROM silver_layer.crm_customer_info AS crm_ci
LEFT JOIN silver_layer.erb_customer_az12 AS erb_caz12 
ON crm_ci.customer_key = erb_caz12.customer_id
LEFT JOIN silver_layer.erb_location_a101 erb_loc
ON crm_ci.customer_key = erb_loc.customer_id
) AS temp
WHERE customer_gender = 'Unknown'

-- another check for the refined one 
SELECT COUNT(customer_gender)
FROM
(SELECT 
crm_ci.customer_id,
crm_ci.customer_key,
crm_ci.customer_first_name,
crm_ci.customer_last_name,
crm_ci.customer_marital_status,
        -- trust crm customer gender if differrnce appears 
-- CASE    WHEN crm_ci.customer_gender != 'Unknown' AND erb_caz12.gender IS NOT NULL
--         AND crm_ci.customer_gender != erb_caz12.gender THEN crm_ci.customer_gender
--         -- trust crm customer gender if erb gender null
--         WHEN erb_caz12.gender IS NULL AND crm_ci.customer_gender != 'Unknown'
--         THEN crm_ci.customer_gender
        
--         WHEN crm_ci.customer_gender = 'Unknown' AND erb_caz12.gender IS NOT NULL 
--         THEN erb_caz12.gender

--         ELSE crm_ci.customer_gender
        -- same with more simplicity and clean
CASE    WHEN crm_ci.customer_gender != 'Unknown' THEN crm_ci.customer_gender
        ELSE COALESCE(erb_caz12.gender, 'Unknown')
END AS customer_gender,
crm_ci.customer_create_date,
erb_caz12.birth_date,
erb_loc.country
FROM silver_layer.crm_customer_info AS crm_ci
LEFT JOIN silver_layer.erb_customer_az12 AS erb_caz12 
ON crm_ci.customer_key = erb_caz12.customer_id
LEFT JOIN silver_layer.erb_location_a101 erb_loc
ON crm_ci.customer_key = erb_loc.customer_id
) AS temp
WHERE customer_gender = 'Unknown';

--- checking the customer info view
SELECT * FROM gold_layer.dim_customer_info
SELECT DISTINCT customer_gender FROM gold_layer.dim_customer_info
