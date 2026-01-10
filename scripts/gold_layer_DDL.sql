
CREATE VIEW gold_layer.dim_customer_info AS 

SELECT 
-- add surrogate key
ROW_NUMBER() OVER (ORDER BY crm_ci.customer_id) AS customer_key,
crm_ci.customer_id AS customer_id,
crm_ci.customer_key AS customer_number,
crm_ci.customer_first_name AS first_name,
crm_ci.customer_last_name AS last_name,
crm_ci.customer_marital_status AS marital_status,
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
crm_ci.customer_create_date AS customer_create_date,
erb_caz12.birth_date As customer_birth_date,
erb_loc.country AS customer_country
FROM silver_layer.crm_customer_info AS crm_ci
LEFT JOIN silver_layer.erb_customer_az12 AS erb_caz12 
ON crm_ci.customer_key = erb_caz12.customer_id
LEFT JOIN silver_layer.erb_location_a101 erb_loc
ON crm_ci.customer_key = erb_loc.customer_id

