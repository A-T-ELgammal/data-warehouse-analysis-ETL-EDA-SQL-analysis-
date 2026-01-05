SELECT * From bronze_layer.crm_customer_info;

--> cleaning the crm_customer_info table

SELECT customer_id, COUNT(*) 
FROM bronze_layer.crm_customer_info
GROUP BY customer_id
HAVING COUNT(*) > 1 OR customer_id IS NULL;


ALTER TABLE bronze_layer.crm_customer_info 
RENAME COLUMN customer_material_status TO customer_marital_status;

SELECT DISTINCT customer_marital_status
FROM bronze_layer.crm_customer_info

SELECT * 
FROM bronze_layer.crm_customer_info
WHERE customer_id = 29466;
--> selecting just latest update of customer info 
SELECT * 
FROM(
SELECT *
, RANK() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC) AS updated_state
FROM bronze_layer.crm_customer_info
)AS ranked_customers WHERE updated_state = 1;

--> customer_info cleaning query
SELECT 
customer_id, customer_key,
TRIM(customer_first_name) AS customer_first_name,
TRIM(customer_last_name) AS customer_last_name,
TRIM(customer_gender) AS customer_gender,
TRIM(customer_marital_status) AS customer_marital_status
,CASE   WHEN UPPER(customer_gender) = 'M' THEN 'Male'
        WHEN UPPER(customer_gender) = 'F' THEN 'Female'
        ELSE 'Unknown'
 END customer_gender,
 CASE   WHEN UPPER(customer_marital_status) = 'S' THEN 'Single'
        WHEN UPPER(customer_marital_status) = 'M' THEN 'Married'
        ELSE 'Unkown'
 END customer_marital_status,
customer_create_date
FROM(
SELECT *
, RANK() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC) AS updated_state
FROM bronze_layer.crm_customer_info
)AS ranked_customers WHERE updated_state = 1;

