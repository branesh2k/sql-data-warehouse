/*
------------------------------------------------------------------------
BRONZE LAYER DATA QUALITY CONTROL AND VALIDATION SCRIPT
------------------------------------------------------------------------
 SCRIPT PURPOSE:
 *	This script performs comprehensive data quality checks on bronze layer tables.
 *	Identifies data issues, inconsistencies, and violations before silver transformation.
 *	Ensures data integrity and quality standards for the medallion architecture.
 
 QC CATEGORIES COVERED:
 *	Primary Key Validation: Duplicates and NULL checks
 *	Data Type Validation: Format consistency and range checks
 *	Referential Integrity: Foreign key relationships validation
 *	Business Rules: Logic validation (sales = quantity * price)
 *	Data Standardization: Consistent values and formats
 *	Data Cleanliness: Whitespace and formatting issues
 
 TABLES VALIDATED:
 *	CRM Tables: crm_cust_info, crm_prd_info, crm_sales_details
 *	ERP Tables: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2
 
 USAGE:
 *	Run this script after bronze layer data loading
 *	Execute before running silver layer transformations
 *	Use results to identify and fix data quality issues
 
 WARNING:
 *	This script only identifies issues - it does not fix them
 *	Manual intervention or data correction may be required
 *	Some checks reference silver tables for cross-layer validation
*/

-- Analyzing DATA from bronze layer tables.
-------------------------------------------------------------------------
-- ========================================================================
-- CRM CUSTOMER INFO VALIDATION
-- ========================================================================

--Identify null or duplicates in Primary Key
--Expectation : No result
SELECT cst_id,
count(*) 
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS null;



--Look for unwanted spaces in columns that has string datatypes
--Expectation : No result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != trim(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info cci 
WHERE cst_lastname  != trim(cst_lastname);

SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != trim(cst_marital_status);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != trim(cst_gndr);



--Data standardization and Consistency[columns with low cardinality]
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

-- ========================================================================
-- CRM PRODUCT INFO VALIDATION
-- ========================================================================

SELECT * FROM bronze.crm_prd_info;

--Identify null or duplicates in Primary Key
--Expectation : No result
SELECT prd_id ,count(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 or prd_id IS null;



--Look for unwanted spaces in columns that has string datatypes
--Expectation : No result
SELECT prd_key FROM bronze.crm_prd_info
WHERE prd_key != trim(prd_key)

SELECT prd_nm  FROM bronze.crm_prd_info
WHERE prd_nm != trim(prd_nm)

SELECT prd_line  FROM bronze.crm_prd_info
WHERE prd_line != trim(prd_line) --need trim



--Check for NULL or -ve numbers
--Expectation : No result
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0; --if null change to 0



--Data standardization and Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;



--Check for invalid date orders
SELECT * FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

-- ========================================================================
-- CRM SALES DETAILS VALIDATION
-- ========================================================================

SELECT * FROM bronze.crm_sales_details;

--There can be multiple products for same order num{so no point of finding duplicates}
--Look for unwanted spaces
SELECT sls_ord_num FROM bronze.crm_sales_details
WHERE sls_ord_num != trim(sls_ord_num);



--next 2 columns are foreign key so we check if any not exists in its relative table
--Expectation : no result
SELECT * FROM bronze.crm_sales_details csd
WHERE csd.sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

SELECT * FROM bronze.crm_sales_details csd 
WHERE csd.sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);



--Check for invalid dates( since it is in integer format)
SELECT sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR length(sls_order_dt::text) != 8
OR sls_order_dt < 19900101
OR sls_order_dt > 20500101;

SELECT sls_ship_dt FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR length(sls_ship_dt::text) != 8
OR sls_ship_dt < 19900101
OR sls_ship_dt > 20500101;

SELECT sls_due_dt FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR length(sls_due_dt::text) != 8
OR sls_due_dt < 19900101
OR sls_due_dt > 20500101;

--Check for invalid dates
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


--Business rule: sales = quantity * price[ they cannot be -ve, 0 or null]
SELECT sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_quantity  IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price<=0
OR sls_sales != sls_quantity * sls_price
ORDER BY sls_sales,sls_quantity,sls_price;

-- ========================================================================
-- ERP CUSTOMER DATA (AZ12) VALIDATION
-- ========================================================================

--since the table has relation we need to check to relate it through coloumns by looking data
SELECT * FROM bronze.erp_cust_az12;
SELECT * FROM silver.crm_cust_info;


--Identify null or duplicates in Primary Key
--Expectation : No result
SELECT cid,count(*) FROM bronze.erp_cust_az12
GROUP BY cid
HAVING count(*)>1 OR cid IS NULL;

SELECT DISTINCT length(cid) FROM bronze.erp_cust_az12;



--Check for invalid dates
SELECT bdate FROM bronze.erp_cust_az12
WHERE bdate < now()::date - INTERVAL '100 years' OR bdate > now()::date;



--Look for unwanted spaces in columns that has string datatypes
--Expectation : No result
SELECT gen FROM bronze.erp_cust_az12
WHERE gen != trim(gen);

SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

-- ========================================================================
-- ERP LOCATION DATA (A101) VALIDATION
-- ========================================================================

--since the table has relation we need to check to relate it through coloumns by looking data
SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM bronze.crm_cust_info cci ;


--Identify null or duplicates in Primary Key
--Expectation : No result
SELECT cid,count(*) FROM bronze.erp_loc_a101 ela
GROUP BY cid
HAVING count(*)>1 OR cid IS NULL;


SELECT DISTINCT length(cid) FROM bronze.erp_loc_a101;



--Data standardization and Consistency
SELECT DISTINCT cntry FROM bronze.erp_loc_a101
ORDER BY cntry;

-- ========================================================================
-- ERP PRODUCT CATEGORY DATA (G1V2) VALIDATION
-- ========================================================================

--since the table has relation we need to check to relate it through coloumns by looking data
SELECT * FROM bronze.erp_px_cat_g1v2;
SELECT * FROM silver.crm_prd_info;

--Identify null or duplicates in Primary Key
--Expectation : No result
SELECT id, count(*) FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING count(*) > 1 OR id IS null; 



--Check for unwanted spaces
--Expectation : NO result
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE trim(cat)!=cat OR trim(subcat) != subcat OR trim(maintenance) != maintenance;


--Data standardization and Consistency
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2;

-- ========================================================================
-- QC VALIDATION COMPLETED
-- ========================================================================
-- Review all query results above:
-- - Empty results indicate good data quality
-- - Any returned rows indicate data quality issues requiring attention
-- - Fix identified issues before proceeding to silver layer transformation