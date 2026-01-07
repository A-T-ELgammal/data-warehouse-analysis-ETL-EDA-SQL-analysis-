CREATE OR REPLACE PROCEDURE silver_layer.silver_load()
LANGUAGE plpgsql
AS $$
DECLARE 
    start_time TIMESTAMP;
    patch_start_time TIMESTAMP;
    end_time TIMESTAMP;
    patch_end_time TIMESTAMP;
    duration INTERVAL;

BEGIN
    BEGIN
        patch_start_time:=clock_timestamp();
        ------------------------------------------------------------------
        --> customer info clean and insertion to silver layer
            BEGIN
                RAISE NOTICE '=================================================';
                RAISE NOTICE 'Loading silver Layer';
                RAISE NOTICE '=================================================';
                RAISE NOTICE 'Truncating table: silver_layer.crm_customer_info';
                TRUNCATE TABLE silver_layer.crm_customer_info;    
                start_time:= clock_timestamp();
                RAISE NOTICE 'Start loading data into the table: silver_layer.crm_customer_info at // %', start_time;
                end_time:= clock_timestamp();
                duration:= end_time - start_time; 
                BEGIN
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
                            FROM bronze_layer.crm_customer_info
                     )
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
                END;
                RAISE NOTICE 'Successfully inserting silver_layer.crm_customer_info';
                RAISE NOTICE '------------------------------------------------------';
                -------------------------------------------------------------
                --> crm product_info cleaning and inserting to silver layer 
                RAISE NOTICE 'Truncating table: silver_layer.crm_product_info';
                TRUNCATE TABLE silver_layer.crm_product_info;
                start_time:= clock_timestamp();
                RAISE NOTICE 'Start loading data into the table: silver_layer.crm_product_info at // %', start_time;
                end_time:= clock_timestamp();
                duration:= end_time - start_time; 
                BEGIN
                     INSERT INTO silver_layer.crm_product_info(
                     product_id ,
                     category_id,
                     product_key,
                     product_name,
                     product_cost,
                     product_line,
                     product_start_date ,
                     product_end_date
                     )
                     --> final query for crm product info table 
                     SELECT 
                     product_id,
                     REPLACE(SUBSTRING(product_key, 1,5),'-','_') AS category_id,
                     REPLACE(SUBSTRING(product_key, 7, LENGTH(product_key)), '_', '-') AS product_key,
                     product_name,
                     COALESCE(product_cost, 0) AS product_cost,
                     CASE UPPER(TRIM(product_line))  
                                   WHEN 'S' THEN 'other Sales'
                                   WHEN 'T' THEN 'Touring'
                                   WHEN 'M' THEN 'Mountain'
                                   WHEN 'R' THEN 'Road'
                                   ELSE 'Unkown'
                     END AS product_line,
                     product_start_date ,
                     LEAD(product_start_date) OVER (PARTITION BY product_key ORDER BY product_end_date)-1  AS product_end_date
                     FROM bronze_layer.crm_product_info;
                END;
                RAISE NOTICE 'Successfully inserting silver_layer.crm_product_info';
                RAISE NOTICE '------------------------------------------------------';
                ----------------------------------------------
                --> cleaning and inserting crm sales table
                RAISE NOTICE 'Truncating table: silver_layer.crm_sales_info';
                TRUNCATE TABLE silver_layer.crm_sales_info;
                start_time:= clock_timestamp();
                RAISE NOTICE 'Start loading data into the table: silver_layer.crm_sales_info at // %', start_time;
                end_time:= clock_timestamp();
                duration:= end_time - start_time; 
                BEGIN
                     INSERT INTO silver_layer.crm_sales_info(
                                   sales_order_number,
                                   sales_product_key,
                                   sales_customer_id,
                                   sales_order_date,
                                   sales_shipping_date,
                                   sales_due_date,
                                   sales_total_sales,
                                   sales_quantity,
                                   sales_price
                     )
                     SELECT 
                     sales_order_number,
                     sales_product_key,
                     sales_customer_id,
                     -- NULLIF(sales_order_date, 0) AS sales_order_date,
                     CASE   WHEN sales_order_date = 0 THEN NULL
                                   WHEN LENGTH(CAST(sales_order_date AS VARCHAR)) != 8 THEN NULL
                                   ELSE CAST(CAST(sales_order_date AS VARCHAR) AS DATE)
                                   END AS sales_order_date, 
                                   
                     CASE   WHEN sales_shipping_date = 0 THEN NULL
                                   WHEN LENGTH(CAST(sales_shipping_date AS VARCHAR)) != 8 THEN NULL
                                   ELSE CAST(CAST(sales_shipping_date AS VARCHAR) AS DATE)
                                   END AS sales_shipping_date,
                     CASE   WHEN sales_due_date = 0 THEN NULL
                                   WHEN LENGTH(CAST(sales_due_date AS VARCHAR)) != 8 THEN NULL
                                   ELSE CAST(CAST(sales_due_date AS VARCHAR) AS DATE)
                                   END AS sales_due_date,
                     -- Recalculate sales if original value is missing or incorrect
                     CASE   WHEN sales_total_sales IS NULL OR sales_total_sales <= 0 
                                                 OR sales_total_sales != sales_quantity * ABS(sales_price) 
                                                 THEN sales_quantity * ABS(sales_price)
                                   ELSE sales_total_sales
                                   END AS sales_total_sales,
                     sales_quantity,
                     ---- Derive price if original value is invalid
                     CASE WHEN sales_price IS NULL OR sales_price <= 0 
                                   THEN sales_total_sales / COALESCE(sales_quantity, 0)
                                   ELSE sales_price
                                   END AS sales_price
                                   
                     FROM bronze_layer.crm_sales_info;
                END;
                RAISE NOTICE 'Successfully inserting silver_layer.crm_sales_info';
                RAISE NOTICE '------------------------------------------------------';
                ----------------------------------------------
                --> cleaning and inserting erb_ customer_az12
                RAISE NOTICE 'Truncating table: silver_layer.erb_customer_az12';
                TRUNCATE silver_layer.erb_customer_az12;
                start_time:= clock_timestamp();
                RAISE NOTICE 'Start loading data into the table: silver_layer.erb_customer_az12 at // %', start_time;
                end_time:= clock_timestamp();
                duration:= end_time - start_time; 
                
                BEGIN
                     INSERT INTO silver_layer.erb_customer_az12(
                                   customer_id,
                                   birth_date,
                                   gender
                     )
                     select
                     CASE   WHEN customer_id ~ '^NAS' THEN SUBSTRING((TRIM(customer_id)),4, LENGTH((TRIM(customer_id))))
                                   ELSE customer_id
                     END AS customer_id,
                     CASE WHEN birth_date > CURRENT_DATE 
                                   THEN NULL
                                   ELSE birth_date
                     END AS birth_date,
                     CASE WHEN TRIM(gender) = 'M' THEN 'Male'
                     WHEN TRIM (gender) = 'F' THEN 'Female'
                     ELSE TRIM(gender)
                     END AS gender
                     FROM bronze_layer.erb_customer_az12;
                END;
                RAISE NOTICE 'Successfully inserting silver_layer.erb_customer_az12';
                RAISE NOTICE '------------------------------------------------------';
                ----------------------------------------------
                --> cleaning ans inserting erb_location_a101
                RAISE NOTICE 'Truncating table: silver_layer.erb_location_a101';
                TRUNCATE TABLE silver_layer.erb_location_a101;
                start_time:= clock_timestamp();
                RAISE NOTICE 'Start loading data into the table: silver_layer.erb_location_a101 at // %', start_time;
                end_time:= clock_timestamp();
                duration:= end_time - start_time; 
                
                BEGIN
                     INSERT INTO silver_layer.erb_location_a101(
                            customer_id,
                            country
                     )
                     SELECT REGEXP_REPLACE((TRIM(customer_id)), '-','') AS customer_id,
                     CASE   WHEN country IN ('US', 'USA', 'United States') THEN 'USA'
                            WHEN country = 'DE' THEN 'Germany'
                            WHEN country = ' ' OR country IS NULL THEN NULL
                            ELSE country
                     END AS country
                     FROM bronze_layer.erb_location_a101;
                END;
                RAISE NOTICE 'Successfully inserting silver_layer.erb_location_a101';
                RAISE NOTICE '------------------------------------------------------';
                --------------------------------------------
                --> cleaning and inserting erb_category_glv2
                RAISE NOTICE 'Truncating table: silver_layer.erb_category_glv2';
                TRUNCATE TABLE silver_layer.erb_category_glv2;
                start_time:= clock_timestamp();
                RAISE NOTICE 'Start loading data into the table: silver_layer.erb_category_glv2 at // %', start_time;
                end_time:= clock_timestamp();
                duration:= end_time - start_time; 
                BEGIN
                     INSERT INTO silver_layer.erb_category_glv2
                     (
                                   category_id,
                                   category,
                                   sub_category,
                                   maintenance
                     )
                     SELECT * FROM bronze_layer.erb_category_glv2;
                END;
                RAISE NOTICE 'Successfully inserting silver_layer.erb_category_glv2';
                RAISE NOTICE '------------------------------------------------------';
            END;
            patch_end_time:= clock_timestamp();
            duration:= patch_end_time - patch_start_time;
            RAISE NOTICE '=====================================================';
            RAISE NOTICE 'all patch of silver layer data loading duration is :   % ', duration;
    END;
END; 
$$;

CALL silver_layer.silver_load();