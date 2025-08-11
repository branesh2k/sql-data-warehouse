
/*
------------------------------------------------------------------------
SILVER LAYER DATA TRANSFORMATION AND LOADING STORED PROCEDURE
------------------------------------------------------------------------
 PROCEDURE PURPOSE:
 *	This stored procedure transforms and loads data from bronze to silver layer tables.
 *	It applies data cleansing, standardization, and business logic transformations.
 *	Silver layer contains cleaned, validated, and enriched data ready for analytics.
 
 TRANSFORMATION LOGIC:
 *	CRM Customer: Deduplication, standardized marital status and gender codes
 *	CRM Product: Category extraction, product line standardization, end date calculation
 *	CRM Sales: Date format conversion, sales amount validation and recalculation
 *	ERP Customer: ID format standardization, date validation, gender normalization
 *	ERP Location: ID cleanup, country code standardization
 *	ERP Product Category: Direct mapping with minimal transformation
 
 USAGE:
 *	CALL silver.load_silver();
 
 DEPENDENCIES:
 *	Requires bronze layer tables to be populated first
 *	Execute bronze.load_bronze() before running this procedure
 
 RETURNS:
 *	Console messages with processing duration for each table transformation.
 
 WARNING:
 *	This procedure truncates all silver tables before loading transformed data.
 *	Ensure bronze layer data is current and validated before execution.
 *	Transformation logic may change data values based on business rules.
*/


-- Insert transformed data in silver layer


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
BEGIN
	-- Log procedure start
	batch_start_time := now();
	RAISE NOTICE 'Starting Silver layer Data load at %', NOW();
	

	-- ========================================================================
	-- TRANSFORM CRM DATA SOURCES
	-- ========================================================================

	-- Transform and Loading silver.crm_cust_info
	start_time := now();
	RAISE NOTICE 'Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	
	RAISE NOTICE 'Transform and Inserting Data into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info (cst_id ,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
	SELECT 
	cst_id,
	cst_key,
	trim(cst_firstname) AS cst_firstname,
	trim(cst_lastname) AS cst_lastname,
	CASE 
		WHEN upper(trim(cst_marital_status)) = 'M' THEN 'Married'
		WHEN upper(trim(cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
	END cst_marital_status,
	CASE 
		WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
		WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr,
	cst_create_date
	FROM (
	SELECT *,
	row_number() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_list
	FROM bronze.crm_cust_info)
	WHERE flag_list = 1 AND cst_id IS NOT null;
	
	end_time := now();
	RAISE NOTICE '>> Transformation and Load duration: % seconds', age(end_time,start_time);
	raise notice '==============================================';
	--=========================================================================================================
	
	-- Transform and Loading silver.crm_prd_info
	start_time := now();
	RAISE NOTICE 'Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	
	RAISE NOTICE 'Transform and Inserting Data into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
	SELECT
	prd_id,	
	REPLACE(substring(prd_key, 1, 5 ), '-', '_') AS  cat_id,
	substring(prd_key, 7, length(prd_key)) AS prd_key,
	prd_nm,
	coalesce(prd_cost,0) AS prd_cost,
	CASE upper(trim(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'R' THEN 'Road'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,
	LEAD(prd_start_dt) over (PARTITION BY prd_key ORDER BY prd_start_dt) - 1  AS prd_end_dt
	FROM bronze.crm_prd_info
	ORDER BY prd_id;
	
	end_time := now();
	RAISE NOTICE '>> Transformation and Load duration: % seconds', age(end_time,start_time);
	raise notice '==============================================';
	--==========================================================================================================
	
	-- Transform and Loading silver.crm_sales_details
	start_time := now();
	RAISE NOTICE 'Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	
	RAISE NOTICE 'Transform and Inserting Data into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
	SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE
		WHEN sls_order_dt = 0 OR length(sls_order_dt::text) !=8 THEN NULL 
		ELSE to_date(sls_order_dt::text,'YYYYMMDD')
	END AS sls_order_dt,
	to_date(sls_ship_dt::text, 'YYYYMMDD') AS sls_ship_dt,
	to_date(sls_due_dt::text, 'YYYYMMDD') AS sls_due_dt,
	CASE 
		WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * abs(sls_price)
		THEN sls_quantity * abs(sls_price) 
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE
		 WHEN sls_price <=0  OR sls_price IS NULL THEN sls_sales::decimal/coalesce(sls_quantity,0) 
		 ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;
	
	end_time := now();
	RAISE NOTICE '>> Transformation and Load duration: % seconds', age(end_time,start_time);
	raise notice '==============================================';
	
	
	-- ========================================================================
	-- TRANSFORM ERP DATA SOURCES
	-- ========================================================================
	
	-- Transform and Loading silver.erp_cust_az12
	start_time := now();
	RAISE NOTICE 'Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	
	RAISE NOTICE 'Transform and Inserting Data into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
	SELECT 
	CASE
		WHEN length(cid) = 13 THEN substr(cid,4,length(cid))
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate >now()::date THEN NULL
		ELSE bdate
	END AS bdate,
	CASE
		WHEN upper(trim(gen)) IN ('M','MALE') THEN 'Male'
		WHEN upper(trim(gen)) IN ('F','FEMALE') THEN 'Female'
		ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12;
	
	end_time := now();
	RAISE NOTICE '>> Transformation and Load duration: % seconds', age(end_time,start_time);
	raise notice '==============================================';
	--====================================================================================================================
	
	-- Transform and Loading silver.erp_loc_a101
	start_time := now();
	RAISE NOTICE 'Truncating Table: silver.erp_loc_a101 ';
	TRUNCATE TABLE silver.erp_loc_a101;
	
	RAISE NOTICE 'Transform and Inserting Data into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(cid,cntry)
	SELECT
	replace(cid,'-','') AS cid,
	CASE
		WHEN trim(cntry) = 'DE' THEN 'Germany'
		WHEN trim(cntry) IN ('US','USA') THEN 'United States'
		WHEN trim(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE trim(cntry)
	END as cntry
	FROM bronze.erp_loc_a101;
	
	end_time := now();
	RAISE NOTICE '>>Transformation and Load duration: % seconds', age(end_time,start_time);
	raise notice '==============================================';
	--====================================================================================================================
	
	-- Transform and Loading silver.erp_px_cat_g1v2
	start_time := now();
	RAISE NOTICE 'Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	
	RAISE NOTICE 'Transform and Inserting Data into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
	SELECT
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
	
	end_time := now();
	RAISE NOTICE '>> Transformation and load duration: % seconds', age(end_time,start_time);
	
	-- ========================================================================
	-- COMPLETION SUMMARY
	-- ========================================================================
	batch_end_time := now();
	RAISE NOTICE '========================================================================';
	RAISE NOTICE 'Silver layer data transformation completed at %', NOW();
	RAISE NOTICE 'Total processing time: % seconds', age(batch_end_time,batch_start_time);
	RAISE NOTICE 'All bronze data successfully transformed and loaded to silver layer';
	RAISE NOTICE '========================================================================';

EXCEPTION
	WHEN OTHERS THEN
	    RAISE EXCEPTION 'Error in Silver layer data transformation: %', SQLERRM;
END;
$$;