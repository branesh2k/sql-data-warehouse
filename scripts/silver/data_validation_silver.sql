
/*
------------------------------------------------------------------------
SILVER LAYER DATA VALIDATION AND QUALITY ASSURANCE SCRIPT
------------------------------------------------------------------------
 SCRIPT PURPOSE:
 *	This script performs comprehensive data validation on silver layer tables.
 *	Validates transformation results and ensures data quality after silver processing.
 *	Confirms that business rules and data cleansing operations were applied correctly.
 
 VALIDATION SCOPE:
 *	Post-Transformation Quality: Verifies cleaned and standardized data
 *	Business Rule Compliance: Ensures transformations meet business requirements
 *	Data Integrity: Validates relationships and constraints after processing
 *	Transformation Accuracy: Confirms expected data format conversions
 *	Standardization Verification: Checks categorical value consistency
 *	Referential Integrity: Validates cross-table relationships
 
 TABLES VALIDATED:
 *	CRM Tables: crm_cust_info, crm_prd_info, crm_sales_details
 *	ERP Tables: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2
 
 EXPECTED RESULTS:
 *	All validation queries should return NO RESULTS if transformations are successful
 *	Any results indicate transformation issues or data quality problems
 *	Compare with bronze QC results to verify improvement in data quality
 
 QUALITY IMPROVEMENTS VERIFIED:
 *	✓ Duplicates removed through deduplication logic
 *	✓ Categorical values standardized (M/F → Male/Female)
 *	✓ Date formats converted from integer to proper dates
 *	✓ Business calculations validated (sales = quantity * price)
 *	✓ NULL handling and default value assignments
 *	✓ Data trimming and whitespace cleanup
 
 USAGE:
 *	Run this script after silver layer transformation completion
 *	Execute before proceeding to gold layer
 *	Use results to validate transformation effectiveness
 
 WARNING:
 *	This script validates silver layer data quality and transformation results
 *	Any failures indicate issues in transformation logic that need correction
 *	Do not proceed to gold layer if significant validation failures exist
*/

--FINAL CHECK silver tables data quality check
------------------------------------------------------------------------
-- ========================================================================
-- SILVER CRM CUSTOMER INFO VALIDATION
-- ========================================================================

--Identify null or duplicates in Primary Key
--Expectation : No result
SELECT cst_id,
count(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS null;



--Look for unwanted spaces in columns that has string datatypes
--Expectation : No result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != trim(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info cci 
WHERE cst_lastname  != trim(cst_lastname);

SELECT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != trim(cst_marital_status);

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != trim(cst_gndr);



--Data standardization and Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;


SELECT * FROM silver.crm_cust_info LIMIT 1000;

-- ========================================================================
-- SILVER CRM PRODUCT INFO VALIDATION
-- ========================================================================

--Check for NULL or -ve numbers
--Expectation : No result
SELECT prd_cost FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;



--Data standardization and Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;



--Check for invalid date orders
SELECT * FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

SELECT * FROM silver.crm_prd_info cpi  LIMIT 1000;

-- ========================================================================
-- SILVER CRM SALES DETAILS VALIDATION
-- ========================================================================

--There can be multiple products for same order num{so no point of finding duplicates}
--Look for unwanted spaces
SELECT sls_ord_num FROM silver.crm_sales_details
WHERE sls_ord_num != trim(sls_ord_num);



--next 2 columns are foreign key so we check if any not exists in its relative table
--Expectation : no result
SELECT * FROM silver.crm_sales_details csd
WHERE csd.sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

SELECT * FROM silver.crm_sales_details csd 
WHERE csd.sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);



--Check for invalid dates( since it is in integer format)
SELECT sls_order_dt FROM silver.crm_sales_details --since we fixed we cannot check with the same script[compare date with 0]
WHERE sls_order_dt <= 0
OR length(sls_order_dt::text) != 8
OR sls_order_dt < 19900101
OR sls_order_dt > 20500101;

SELECT sls_ship_dt FROM silver.crm_sales_details --since we fixed we cannot check with the same script[compare date with 0]
WHERE sls_ship_dt <= 0
OR length(sls_ship_dt::text) != 8
OR sls_ship_dt < 19900101
OR sls_ship_dt > 20500101;

SELECT sls_due_dt FROM silver.crm_sales_details --since we fixed we cannot check with the same script[compare date with 0]
WHERE sls_due_dt <= 0
OR length(sls_due_dt::text) != 8
OR sls_due_dt < 19900101
OR sls_due_dt > 20500101;

--Check for invalid dates
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


--Business rule: sales = quantity * price[ they cannot be -ve, 0 or null]
SELECT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales IS NULL OR sls_quantity  IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price<=0
OR sls_sales != sls_quantity * sls_price
ORDER BY sls_sales,sls_quantity,sls_price;

SELECT * FROM silver.crm_sales_details;

-- ========================================================================
-- SILVER ERP CUSTOMER DATA (AZ12) VALIDATION
-- ========================================================================

--Identify null or duplicates in Primary Key
--Expectation : No result
SELECT cid,count(*) FROM silver.erp_cust_az12
GROUP BY cid
HAVING count(*)>1 OR cid IS NULL;

SELECT DISTINCT length(cid) FROM silver.erp_cust_az12;


--Check for invalid dates
SELECT bdate FROM silver.erp_cust_az12
WHERE  bdate > now()::date;



--Look for unwanted spaces in columns that has string datatypes
--Expectation : No result
SELECT gen FROM silver.erp_cust_az12
WHERE gen != trim(gen);

SELECT DISTINCT gen
FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12 eca ;

-- ========================================================================
-- SILVER ERP LOCATION DATA (A101) VALIDATION
-- ========================================================================

--Identify null or duplicates in Primary Key
--Expectation : No result
SELECT cid,count(*) FROM silver.erp_loc_a101 ela
GROUP BY cid
HAVING count(*)>1 OR cid IS NULL;


SELECT DISTINCT length(cid) FROM silver.erp_loc_a101;


--Data standardization and Consistency
SELECT DISTINCT cntry FROM silver.erp_loc_a101
ORDER BY cntry;


SELECT * FROM silver.erp_loc_a101;

-- ========================================================================
-- SILVER ERP PRODUCT CATEGORY DATA (G1V2) VALIDATION
-- ========================================================================

--Identify null or duplicates in Primary Key
--Expectation : No result
SELECT id, count(*) FROM silver.erp_px_cat_g1v2
GROUP BY id
HAVING count(*) > 1 OR id IS null; 



--Check for unwanted spaces
--Expectation : NO result
SELECT * FROM silver.erp_px_cat_g1v2
WHERE trim(cat)!=cat OR trim(subcat) != subcat OR trim(maintenance) != maintenance;


--Data standardization and Consistency
SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2;

-- ========================================================================
-- SILVER LAYER VALIDATION COMPLETED
-- ========================================================================
-- Review all query results above:
-- - Empty results indicate successful transformations and good data quality
-- - Any returned rows indicate transformation issues requiring investigation
-- - Compare results with bronze QC to verify data quality improvements
-- - Silver layer is ready for gold layer processing if all validations pass