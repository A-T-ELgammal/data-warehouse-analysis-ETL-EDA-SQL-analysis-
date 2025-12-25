CREATE OR REPLACE PROCEDURE bronze_layer.bronze_load()
LANGUAGE plpgsql
AS $$
DECLARE 
start_time TIMESTAMP;
end_time TIMESTAMP;
patch_start_time TIMESTAMP;
patch_end_time TIMESTAMP;

BEGIN
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '=================================================';

    RAISE NOTICE 'Truncating table: bronze_layer.crm_customer_info';
    TRUNCATE TABLE bronze_layer.crm_customer_info;
    start_time:= NOW();
    RAISE NOTICE 'Start loading data into the table: bronze_layer.crm_customer_info at // %', start_time;
    end_time:= NOW();
    BEGIN
        -- COPY bronze_layer.crm_customer_info FROM '/home/ahmed/projects/data_analysis/data-warehouse-analysis/datasets/source_crm/cust_info.csv' WITH CSV HEADER;
        COPY bronze_layer.crm_customer_info FROM '/var/lib/postgresql/data/cust_info.csv' WITH CSV HEADER;
        RAISE NOTICE 'Loading data successfully at time:%', end_time; 
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'not successfully loading the data with that error: %', SQLERRM;
            RAISE NOTICE 'ending time is: % ', end_time;
    END;
END;
$$;


-- Test
CALL bronze_layer.bronze_load();
SELECT COUNT(*) FROM bronze_layer.crm_customer_info;
