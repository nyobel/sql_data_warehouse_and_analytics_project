/*
====================================================================================================================
Product Report
====================================================================================================================
Purpose:
	- This report consolidates key product metrics and behaviors

Highlights:
	1. Gathers essential fields such as product_name, category, subcategory and cost.
	2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
	3. Aggreagates product-level metrics:
		- Total orders
		- Total sales
		- Total quantity sold
		- Total customers (unique)
		- Lifespan (in months)
	4. Calculates valuable KPIs
		- Recency(months since last sale)
		- Average order revenue
		- Average monthly revenue
====================================================================================================================
*/
-- =============================================================================================
-- Create Report: gold.report_products
-- =============================================================================================
IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
    DROP VIEW
GO
  
CREATE VIEW gold.report_products AS

WITH base_query AS (
/*-----------------------------------------------------------------------------------------
1. Base Query: Retrieves core columns from tables
-------------------------------------------------------------------------------------------*/
SELECT
	f.order_number,
	f.customer_key,
	f.order_date,
	f.sales_amount,
	f.quantity,
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL
), 
product_aggregation AS (
/*-----------------------------------------------------------------------------------------
2. Product Aggreagation: Summarizes key metrics at the product level
-------------------------------------------------------------------------------------------*/
SELECT 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
	MAX(order_date) AS last_order_date,
	COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(sales_amount) AS total_sales,
	SUM(quantity)  AS total_quantity,
	ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 2) AS avg_selling_price
FROM base_query
GROUP BY 
	product_key,
	product_name,
	category,
	subcategory,
	cost
)
/*-----------------------------------------------------------------------------------------
3. Final Query: Combines all product results into one output
-------------------------------------------------------------------------------------------*/
SELECT 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	lifespan,
	last_order_date,
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency_in_months,
	CASE WHEN total_sales > 50000 THEN 'High-Performer'
		 WHEN total_sales >= 10000 THEN 'Mid-Range'
		 ELSE 'Low-Performer'
	END AS product_segments,
	total_orders,
	total_customers,
	total_sales,
	total_quantity,
	avg_selling_price,
	-- Average Order Revenue
	CASE WHEN total_orders = 0 THEN 0
		 ELSE total_sales / total_orders
	END AS avg_order_revenue,
	-- Average Monthly Revenue
	CASE WHEN lifespan = 0 THEN total_sales
		 ELSE total_sales / lifespan
	END AS avg_monthly_revenue
FROM product_aggregation
