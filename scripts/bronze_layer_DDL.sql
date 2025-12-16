CREATE PROCEDURE bronze_layer.bronze_load()
LANGUAGE plpgsql
AS $$
BEGIN

DROP TABLE IF EXISTS bronze_layer.crm_customer_info;
CREATE TABLE bronze_layer.crm_customer_info(
    customer_id INT,
    customer_key VARCHAR(50),
    customer_first_name VARCHAR(50),
    customer_last_name VARCHAR(50),
    customer_material_status VARCHAR(50),
    customer_gender VARCHAR(50),
    customer_create_date DATE
);


-- ALTER TABLE bronze_layer.crm_customer_info
-- DROP COLUMN customer_material_status;

-- ALTER TABLE bronze_layer.crm_customer_info
-- ADD COLUMN customer_marital_status VARCHAR(50);

DROP TABLE IF EXISTS bronze_layer.crm_product_info;
CREATE TABLE bronze_layer.crm_product_info(
    product_id INT,
    product_key VARCHAR(50),
    product_name VARCHAR(50),
    product_cost INT,
    product_line CHAR,
    product_start_date DATE,
    product_end_date DATE
);

DROP TABLE IF EXISTS bronze_layer.crm_sales_info;
CREATE TABLE bronze_layer.crm_sales_info(
    sales_order_number VARCHAR(50),
    sales_product_key VARCHAR(50),
    sales_customer_id INT,
    sales_order_date INT,
    sales_shipping_date INT,
    sales_due_date INT,
    sales_total_sales INT,
    sales_quantity INT,
    sales_price INT
);

DROP TABLE IF EXISTS bronze_layer.erb_location_a101;
CREATE TABLE bronze_layer.erb_location_a101(
    customer_id VARCHAR(50),
    birth_date DATE,
    gender VARCHAR(20)
);

DROP TABLE IF EXISTS bronze_layer.erb_location_a101;
CREATE TABLE bronze_layer.erb_location_a101(
    customer_id VARCHAR(50),
    country VARCHAR(50)
);

DROP TABLE IF EXISTS bronze_layer.erb_category_glv2;
CREATE TABLE bronze_layer.erb_category_glv2(
    category_id VARCHAR(50),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    maintenance VARCHAR(50)
);

END;
$$;
