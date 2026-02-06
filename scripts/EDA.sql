-- Dimension exploration 

-- customers & products 

SELECT DISTINCT customer_country
FROM gold_layer.dim_customer_info;

SELECT DISTINCT category, sub_category
FROM gold_layer.dim_product_info
ORDER BY 1,2;

SELECT DISTINCT product_line
FROM gold_layer.dim_product_info;