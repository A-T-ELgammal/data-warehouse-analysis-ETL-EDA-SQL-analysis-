
DROP VIEW IF EXISTS gold_layer.dim_product_info;
CREATE VIEW gold_layer.dim_product_info AS 
SELECT 
ROW_NUMBER() OVER (ORDER BY crm_pi.product_id) AS product_key,
crm_pi.product_id,
crm_pi.product_name,
crm_pi.product_cost,
crm_pi.product_line,
crm_pi.category_id,
erb_cat.category,
erb_cat.sub_category,
crm_pi.product_key AS product_serial,
crm_pi.product_start_date,
crm_pi.product_end_date,
erb_cat.maintenance
FROM silver_layer.crm_product_info crm_pi
LEFT JOIN silver_layer.erb_category_glv2 erb_cat
ON crm_pi.category_id = erb_cat.category_id;
--         WHEN crm_ci.customer_gender = 'Unknown' AND erb_caz12.gender IS NOT NULL 
--         THEN erb_caz12.gender

--         ELSE crm_ci.customer_gender
        -- same with more simplicity and clean
CASE    WHEN crm_ci.customer_gender != 'Unknown' THEN crm_ci.customer_gender
        ELSE COALESCE(erb_caz12.gender, 'Unknown')
END AS customer_gender,
crm_ci.customer_create_date AS customer_create_date,
erb_caz12.birth_date As customer_birth_date,
erb_loc.country AS customer_country
FROM silver_layer.crm_customer_info AS crm_ci
LEFT JOIN silver_layer.erb_customer_az12 AS erb_caz12 
ON crm_ci.customer_key = erb_caz12.customer_id
LEFT JOIN silver_layer.erb_location_a101 erb_loc
ON crm_ci.customer_key = erb_loc.customer_id

--- crm product info table 

--- crm product info table 
DROP VIEW IF EXISTS gold_layer.dim_product_info;
CREATE VIEW gold_layer.dim_product_info AS 
SELECT 
-- surrogate_key
ROW_NUMBER() OVER (ORDER BY crm_pi.product_id) AS product_key,
crm_pi.product_id,
crm_pi.product_name,
crm_pi.product_cost,
crm_pi.product_line,
crm_pi.category_id,
erb_cat.category,
erb_cat.sub_category,
crm_pi.product_key AS product_serial,
crm_pi.product_start_date,
crm_pi.product_end_date,
erb_cat.maintenance
FROM silver_layer.crm_product_info crm_pi
LEFT JOIN silver_layer.erb_category_glv2 erb_cat
ON crm_pi.category_id = erb_cat.category_id



---------------------------------------------
---------crm_ sales_ info--------------------
DROP VIEW IF EXISTS gold_layer.fact_sales_info;
CREATE VIEW gold_layer.fact_sales_info AS  
SELECT 
sls.sales_order_number AS order_number,
pr.product_key AS product_key,
ci.customer_key AS customer_key,        
sls.sales_order_date AS order_date,
sls.sales_shipping_date AS shipping_date,
sls.sales_due_date AS due_date,
sls.sales_total_sales AS total_sales,
sls.sales_quantity AS quantity,
sls.sales_price AS price
FROM silver_layer.crm_sales_info sls
LEFT JOIN gold_layer.dim_product_info pr
ON sls.sales_product_key = CAST(pr.product_serial AS VARCHAR(50))
LEFT JOIN gold_layer.dim_customer_info ci 
ON CAST(sls.sales_customer_id AS VARCHAR(50)) = ci.customer_id


