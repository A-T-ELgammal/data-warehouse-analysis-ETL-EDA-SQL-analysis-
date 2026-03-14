# Data Analysis Report: Global Sales & Customer Intelligence

**Date:** March 14, 2026  
**Project:** Medallion Architecture Data Pipeline (Gold Layer Analysis)

---

## 1. Executive Summary

This report provides a strategic overview of store performance, customer demographics, and product trends based on data spanning 2010 to 2014. Key insights highlight the dominance of the **Bikes** category, the growing purchasing power of the **Australian** market, and a steady year-over-year increase in revenue and customer acquisition.

---

## 2. Exploration & Findings

### First and Last Order Dates

The analysis confirms that the operational data spans from 2010 to 2014. Specifically, the second analysis reveals a time range of approximately **3 years and 30 days**.

![First and Last Order Dates](images/report_output/first&last_order_date.png)
![Order Full Time Range](images/report_output/order_full_time_range.png)

### Customer Age Range

Demographic data reveals a broad age distribution among the customer base. The youngest and oldest customer records clarify an age span of approximately **70 years, 4 months, and 15 days**.

![Customer Age Range](images/report_output/customer_age_range.png)

### Key Measurement Exploration

The following metrics provide a high-level view of our total scale, including total orders, store customers, product counts, total sales, and average pricing.

![Key Measurements Exploration](images/report_output/measure_exploration.png)

### Customers by Country

The **USA** represents our largest customer segment, followed by **Australia** and the **United Kingdom**.

![Customer by Country](images/report_output/customer_by_country.png)

### Customers by Gender

The data indicates that Male customers outnumber Female customers. However, a significant "Unknown" category exists for customers who chose not to identify their gender, which could introduce minor bias in gender-based targeting.

![Customer by Gender](images/report_output/customer_by_gender.png)

### Product Count by Category

The store maintains a higher volume of products in the **Components** section compared to others. The sub-category exploration below highlights the specific breakdown of these parts.

![Products by Category](images/report_output/products_by_cat.png)
![Components Sub-Category Breakdown](images/report_output/Components_sub_cat.png)

### Average Price per Category

Bikes and Components are the most expensive categories, averaging over **$300**, while Clothing and Accessories remain budget-friendly at under **$25**.

![Average Price per Category](images/report_output/price_per_category.png)

### Top 5 Products Sold

The top 5 most sold products are all variations of the **Mountain-200** bike, distinguished by different color options.

![Top 5 Products Sold](images/report_output/top5_products_sold.png)

### Revenue vs. Volume by Country

While the **USA** leads in the total number of products sold, **Australia** generates higher total revenue. Deep-diving into the categories reveals that the USA has higher buying power for high-volume, lower-priced items, putting it in second place for total revenue despite the higher unit count.

![Revenue and Products Sold per Country](images/report_output/revenue&products_sold_per_country.png)
![Total Products by Country and Category](images/report_output/total_prod_by_country_category.png)

### Regional Product Preferences (USA vs. Australia)

The following analysis isolates the Top 5 products for our two largest markets. This comparison demonstrates that while both regions favor the Mountain-200 series, the specific model variations and quantities differ, reflecting unique regional consumer preferences and inventory demand.

![Top 5 Sold Products USA vs AUS](images/report_output/top5_sold_products_USA_AUS.png)

### Top 3 Customers by Orders Placed

This ranking identifies our most frequent shoppers by their total order count and their position in the global customer list.

![Top 3 Customers by Orders Placed](images/report_output/top3_customer_order_placed.png)

### Annual Sales Performance

Sales have increased gradually year-over-year, with revenue, customer counts, and quantities sold all showing growth from 2010 to 2013.

![Annual Sales Trends](images/report_output/sales&customer&quantities_per_year.png)

### Monthly Sales Seasonality (Running Total)

Analysis of monthly sales shows a steady climb throughout the year. **December** accounts for the highest sales volume, while **January** serves as the lowest point.

![Total Running Sales per Month](images/report_output/total_running_sales_per_months.png)

### Total Sales Percentage by Category

The **Bikes** category is the dominant force in our revenue model, accounting for **96%** of all sales across our stores.

![Total Sales Percentage by Category](images/report_output/total_sales_percentage_by_category.png)

---

## 3. Data Segmentation

### Product Tiering (By Price)

Products are categorized into three cost tiers: **Budget** (Below $100), **Mid-Range** ($100 - $500), and **Premium** (Above $500).

![Product Cost Range Segmentation](images/report_output/cost_range_products.png)

### Customer Segmentation (By Spend)

Customers are segmented into **New**, **Regular**, and **VIP** tiers based on their total lifetime spending.

![Customer Type Segmentation](images/report_output/customer_type_seg.png)

---

## 4. Conclusion

The analysis confirms a growing, bike-centric business. Strategic focus should remain on the high-value Australian market and peak-season preparation for December, while exploring growth in the lower-cost accessories categories.

_Report ending with data-driven visualizations to assist the management team in final decision-making._
