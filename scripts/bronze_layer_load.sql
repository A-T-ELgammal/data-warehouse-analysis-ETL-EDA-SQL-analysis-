CREATE OR REPLACE PROCEDURE bronze_layer.bronze_load()
LANGUAGE plpgsql
AS $$
DECLARE 
start_time TIMESTAMP;
patch_start_time TIMESTAMP;
end_time TIMESTAMP;
patch_end_time TIMESTAMP;
duration INTERVAL;
BEGIN
    patch_start_time:=clock_timestamp();
    BEGIN
        RAISE NOTICE '=================================================';
        RAISE NOTICE 'Loading Bronze Layer';
        RAISE NOTICE '=================================================';

        RAISE NOTICE 'Truncating table: bronze_layer.crm_customer_info';
        TRUNCATE TABLE bronze_layer.crm_customer_info;
        start_time:= clock_timestamp();
        RAISE NOTICE 'Start loading data into the table: bronze_layer.crm_customer_info at // %', start_time;
        end_time:= clock_timestamp();
        duration:= end_time - start_time;
        BEGIN
            COPY bronze_layer.crm_customer_info FROM '/var/lib/postgresql/data/data-warehouse-project/crm_data/cust_info.csv' WITH CSV HEADER;
            RAISE NOTICE 'Loading data successfully at time:%', end_time; 
            duration:= end_time - start_time;
            RAISE NOTICE 'data loading took:  % ', duration;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE 'not successfully loading the data with that error: %', SQLERRM;
                RAISE NOTICE 'ending time is: % ', end_time;

        END;

        BEGIN

            RAISE NOTICE '=================================================';
            RAISE NOTICE 'Truncating table: bronze_layer.crm_product_info';
            TRUNCATE TABLE bronze_layer.crm_product_info;
            start_time:= clock_timestamp();
            RAISE NOTICE 'Start loading data into the table: bronze_layer.crm_product_info at // %', start_time;
            end_time:= clock_timestamp();
            BEGIN
                COPY bronze_layer.crm_product_info FROM '/var/lib/postgresql/data/data-warehouse-project/crm_data/prd_info.csv' WITH CSV HEADER;
                RAISE NOTICE 'Loading data successfully at time:%', end_time; 
                duration:= end_time - start_time;
                RAISE NOTICE 'data loading took:  % ', duration;
            EXCEPTION
                WHEN OTHERS THEN
                    RAISE NOTICE 'not successfully loading the data with that error: %', SQLERRM;
                    RAISE NOTICE 'ending time is: % ', end_time;
            END;
        END;

        BEGIN
            RAISE NOTICE '=================================================';
            RAISE NOTICE 'Truncating table: bronze_layer.crm_sales_info';
            TRUNCATE TABLE bronze_layer.crm_sales_info;
            start_time:= clock_timestamp();
            RAISE NOTICE 'Start loading data into the table: bronze_layer.crm_sales_info at // %', start_time;
            end_time:= clock_timestamp();
            BEGIN
                COPY bronze_layer.crm_sales_info FROM '/var/lib/postgresql/data/data-warehouse-project/crm_data/sales_details.csv' WITH CSV HEADER;
                RAISE NOTICE 'Loading data successfully at time:%', end_time;
                duration:= end_time - start_time;
                RAISE NOTICE 'data loading took:  % ', duration;
            EXCEPTION
                WHEN OTHERS THEN
                    RAISE NOTICE 'not successfully loading the data with that error: %', SQLERRM;
                    RAISE NOTICE 'ending time is: % ', end_time;
            END;
        END;

        BEGIN
            RAISE NOTICE '=================================================';
            RAISE NOTICE 'Truncating table: bronze_layer.erb_location_a101';
            TRUNCATE TABLE bronze_layer.erb_location_a101;
            start_time:= clock_timestamp();
            RAISE NOTICE 'Start loading data into the table: bronze_layer.erb_location_a101 at // %', start_time;
            end_time:= clock_timestamp();
            BEGIN
                COPY bronze_layer.erb_location_a101 FROM '/var/lib/postgresql/data/data-warehouse-project/erp_data/LOC_A101.csv' WITH CSV HEADER;
                RAISE NOTICE 'Loading data successfully at time:%', end_time; 
                duration:= end_time - start_time;
                RAISE NOTICE 'data loading took:  % ', duration;
            EXCEPTION
                WHEN OTHERS THEN
                    RAISE NOTICE 'not successfully loading the data with that error: %', SQLERRM;
                    RAISE NOTICE 'ending time is: % ', end_time;
            END;
        END;

        BEGIN
            RAISE NOTICE '=================================================';
            RAISE NOTICE 'Truncating table: bronze_layer.erb_customer_az12';
            TRUNCATE TABLE bronze_layer.erb_customer_az12;
            start_time:= clock_timestamp();
            RAISE NOTICE 'Start loading data into the table: bronze_layer.erb_customer_az12 at // %', start_time;
            end_time:= clock_timestamp();
            BEGIN
                COPY bronze_layer.erb_customer_az12 FROM '/var/lib/postgresql/data/data-warehouse-project/erp_data/CUST_AZ12.csv' WITH CSV HEADER;
                RAISE NOTICE 'Loading data successfully at time:%', end_time; 
                duration:= end_time - start_time;
                RAISE NOTICE 'data loading took:  % ', duration;
            EXCEPTION
                WHEN OTHERS THEN
                    RAISE NOTICE 'not successfully loading the data with that error: %', SQLERRM;
                    RAISE NOTICE 'ending time is: % ', end_time;
            END;
        END;

        BEGIN
            RAISE NOTICE '=================================================';
            RAISE NOTICE 'Truncating table: bronze_layer.erb_category_glv2';
            TRUNCATE TABLE bronze_layer.erb_category_glv2;
            start_time:= clock_timestamp();
            RAISE NOTICE 'Start loading data into the table: bronze_layer.erb_category_glv2 at // %', start_time;
            end_time:= clock_timestamp();
            BEGIN
                COPY bronze_layer.erb_category_glv2 FROM '/var/lib/postgresql/data/data-warehouse-project/erp_data/PX_CAT_G1V2.csv' WITH CSV HEADER;
                RAISE NOTICE 'Loading data successfully at time:%', end_time; 
                duration:= end_time - start_time;
                RAISE NOTICE 'data loading took:  % ', duration;
            EXCEPTION
                WHEN OTHERS THEN
                    RAISE NOTICE 'not successfully loading the data with that error: %', SQLERRM;
                    RAISE NOTICE 'ending time is: % ', end_time;
            END;
        END;
    patch_end_time:= clock_timestamp();
    duration:= patch_end_time - patch_start_time;
    RAISE NOTICE '=====================================================';
    RAISE NOTICE 'all patch of bronze layer data loading duration is :   % ', duration;
    END;
END;
$$;



-- calling procedure 
CALL bronze_layer.bronze_load();

--> Test

--> crm_tables:
SELECT COUNT(*) FROM bronze_layer.crm_customer_info;
SELECT COUNT(*) FROM bronze_layer.crm_product_info;
SELECT COUNT(*) FROM bronze_layer.crm_sales_info;


--> erb_tables:
SELECT COUNT(*) FROM bronze_layer.erb_customer_az12;
SELECT COUNT(*) FROM bronze_layer.erb_category_glv2;
SELECT COUNT(*) FROM bronze_layer.erb_location_a101;
