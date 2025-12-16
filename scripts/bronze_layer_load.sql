-- try copying using script not pgadmin

TRUNCATE TABLE bronze_layer.crm_customer_info;
copy bronze_layer.crm_customer_info
FROM '/home/ahmed/projects/data_analysis/data-warehouse-analysis/datasets /source_crm/cust_info.csv'
WITH(DELIMITER ',',FORMAT csv, HEADER);

-- test coping
SELECT *
FROM bronze_layer.crm_customer_info;


copy bronze_layer.crm_product_info(product_id, product_key, product_name, product_cost, product_line, product_start_date, product_end_date) FROM '/home/ahmed/projects/data_analysis/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv' WITH(FORMAT csv, DELIMITER ',', HEADER);
