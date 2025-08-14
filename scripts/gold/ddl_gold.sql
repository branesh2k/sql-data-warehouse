
/*
------------------------------------------------------------------------
CREATE GOLD LAYER VIEWS FOR DATA WAREHOUSE
------------------------------------------------------------------------
 SCRIPT PURPOSE:
 *	This script creates gold layer views implementing dimensional modeling.
 *	Gold layer provides business-ready, denormalized views for analytics.
 *	Implements star schema with dimension and fact views for reporting.
 
 DIMENSIONAL MODEL:
 *	Fact Table: fact_sales (transactions/measures)
 *	Dimensions: dim_customers (SCD Type 1), dim_products (SCD Type 2)
 *	Surrogate Keys: Generated for dimensional relationships
 
 VIEWS CREATED:
 *	gold.dim_customers: Customer dimension with ERP enrichment
 *	gold.dim_products: Product dimension with category hierarchy
 *	gold.fact_sales: Sales transactions with dimensional keys
 
 BUSINESS RULES:
 *	Customer Gender: CRM is master source, ERP as fallback
 *	Product History: Current products only (filtered by end_date)
 *	Sales Measures: Amount, quantity, and price at transaction level
 
 DATA SOURCES:
 *	silver.crm_cust_info: Base customer attributes
 *	silver.erp_cust_az12: Additional customer data
 *	silver.erp_loc_a101: Customer location data
 *	silver.crm_prd_info: Product information
 *	silver.erp_px_cat_g1v2: Product categorization
 *	silver.crm_sales_details: Sales transactions
 
 DEPENDENCIES:
 *	Requires silver layer views to be populated
 
 WARNING:
 *	Running this script will replace existing gold views
 *	Ensure silver layer data is validated before refresh
 *	Views depend on silver schema and specific column names
*/

-- ========================================================================
-- CUSTOMER DIMENSION VIEW
-- ========================================================================
CREATE OR REPLACE VIEW gold.dim_customers AS  
SELECT
row_number() over(ORDER BY cci.cst_id) AS customer_key,  --surrogate key (like a sys generated serial) only used to connect data model not for business
cci.cst_id AS customer_id,
cci.cst_key AS customer_number,
cci.cst_firstname AS first_name,
cci.cst_lastname AS last_name,
ela.cntry AS country,
cci.cst_marital_status AS marital_status,
CASE 
	WHEN cci.cst_gndr !='n/a' THEN cci.cst_gndr  --CRM is the Master for gender info
	ELSE COALESCE(gen, 'n/a')
END AS new_gender,
eca.bdate AS birthdate,
cci.cst_create_date AS create_date
FROM silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca 
ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
ON cci.cst_key = ela.cid;

-- ========================================================================
-- PRODUCT DIMENSION VIEW
-- ========================================================================
CREATE OR REPLACE VIEW gold.dim_products as
SELECT
row_number() over(ORDER BY cpi.prd_start_dt ,cpi.prd_key ) AS product_key,
cpi.prd_id AS product_id,
cpi.prd_key AS product_number,
cpi.prd_nm AS product_name,
cpi.cat_id AS category_id,
epcgv.cat AS category,
epcgv.subcat AS subcategory,
epcgv.maintenance,
cpi.prd_cost AS cost,
cpi.prd_line AS product_line,
cpi.prd_start_dt AS start_date
FROM silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epcgv
ON cpi.cat_id =epcgv.id 
WHERE cpi.prd_end_dt IS NULL; 				-- Filter historical data


-- ========================================================================
-- SALES FACT VIEW
-- ========================================================================
CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT
csd.sls_ord_num AS order_number,
dp.product_key,								-- surrogate key from product table
dc.customer_key,							-- surrogate key from customer table
csd.sls_order_dt AS order_date,
csd.sls_ship_dt AS shipping_date,
csd.sls_due_dt AS due_date,
csd.sls_sales AS sales_amount,
csd.sls_quantity AS quantity,
csd.sls_price AS price
FROM silver.crm_sales_details csd
LEFT JOIN gold.dim_products dp ON csd.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customers dc ON csd.sls_cust_id = dc.customer_id;