/*
================================================================================================
Quality Checks Silver Schema
================================================================================================
Scripts Purpose:
  This script performs various quality checks for data consistency, accuracy, and standardization
  across the 'silver' schema. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these checks for data loading Silver Layer
  - Investigate and resolve any discrepancies found during the checks
*/

-- =============================================================================================
-- Checking 'silver.crm_cust_info'
-- =============================================================================================
----Check for Nulls and Duplicates in the Primary Key
SELECT 
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


---- Check for unwanted spaces
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);--- has spaces

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);---has spaces

SELECT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);---none

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);---none

SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);---none


---- Data Standardization & Consistency
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info

-- =============================================================================================
-- Checking 'silver.crm_prd_info'
-- =============================================================================================
----Check for Nulls and Duplicates in the Primary Key
SELECT * FROM bronze.crm_prd_info;

SELECT 
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


---- Check for unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


--- Check for Nulls or Negative Numbers
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL;


--- Data Standardization & Consistency
SELECT DISTINCT prd_line FROM silver.crm_prd_info;


--- Check For Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- =============================================================================================
-- Checking 'silver.crm_sales_details'
-- =============================================================================================
-- Check for Invalid Date Orders
SELECT 
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


--- Check Data Consistency: Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative

SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT * FROM silver.crm_sales_details;

-- =============================================================================================
-- Checking 'silver.erp_cust_az12'
-- =============================================================================================
-- Identify Out of Range Dates
SELECT 
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

--- Data Standardization & Consistency
SELECT DISTINCT
  gen 
FROM silver.erp_cust_az12;


-- =============================================================================================
-- Checking 'silver.erp_loc_a101'
-- =============================================================================================
--- Data Standardization & Consistency
SELECT DISTINCT 
  cntry 
FROM silver.erp_loc_a101
ORDER BY cntry

-- =============================================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- =============================================================================================
-- Check for unwanted spaces
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
  OR subcat != TRIM(subcat)
  OR maintenance != TRIM(maintenance);

--- Data Standardization & Consistency
SELECT DISTINCT 
  maintenance 
FROM silver.erp_px_cat_g1v2;
