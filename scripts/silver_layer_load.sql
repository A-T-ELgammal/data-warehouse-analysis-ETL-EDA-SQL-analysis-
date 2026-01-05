SELECT * From bronze_layer.crm_customer_info;

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

------------------------------------------------------------------
--> customer info clean and insertion to silver layer

INSERT INTO silver_layer.crm_customer_info(
       customer_id,
       customer_key,
       customer_first_name,
       customer_last_name,
       customer_gender,
       customer_marital_status,
       customer_create_date
)


--> crm customer_info cleaning query -- final query
with latest_customer_update AS (
    SELECT *
    , RANK() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC) AS updated_state
    FROM bronze_layer.crm_customer_info)
SELECT 
customer_id, customer_key,
TRIM(customer_first_name) AS customer_first_name,
TRIM(customer_last_name) AS customer_last_name,
CASE   WHEN UPPER(TRIM(customer_gender)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(customer_gender)) = 'F' THEN 'Female'
        ELSE 'Unknown'
 END AS  customer_gender,
 CASE   WHEN UPPER(TRIM(customer_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(customer_marital_status)) = 'M' THEN 'Married'
        ELSE 'Unkown'
 END AS customer_marital_status ,
 customer_create_date
FROM latest_customer_update WHERE updated_state = 1;

SELECT * FROM silver_layer.crm_customer_info;
-------------------------------------------------------------
