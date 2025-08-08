/*
------------------------------------------------------------------------
BRONZE LAYER DATA LOADING STORED PROCEDURE
------------------------------------------------------------------------
 PROCEDURE PURPOSE:
 *	This stored procedure loads raw data from CSV files into bronze layer tables.
 *	It truncates existing data and performs a full reload from source files.
 *	Bronze layer stores raw data in its original format from CRM and ERP systems.
 
 DATA SOURCES:
 *	CRM CSV Files: cust_info.csv, prd_info.csv, sales_details.csv
 *	ERP CSV Files: CUST_AZ12.csv, LOC_A101.csv, PX_CAT_G1V2.csv
 
 USAGE:
 *	CALL bronze.load_bronze();
 
 RETURNS:
 *	Console messages with row counts for each loaded table.
 
 WARNING:
 *	This procedure truncates all bronze tables before loading.
 *	Ensure CSV files exist at specified paths before execution.
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    crm_cust_count INTEGER;
    crm_prd_count INTEGER;
    crm_sales_count INTEGER;
    erp_cust_count INTEGER;
    erp_loc_count INTEGER;
    erp_px_count INTEGER;
	start_time TIMESTAMP;
    end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;

BEGIN
    -- Log procedure start
	batch_start_time := now();
    RAISE NOTICE 'Starting bronze layer data load at %', NOW();
    RAISE NOTICE '======================================';
    
    -- Load CRM Customer Information
	start_time := now();
    RAISE NOTICE 'Loading CRM customer information...';
	TRUNCATE TABLE bronze.crm_cust_info;
	
	COPY bronze.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
	FROM 'C:\Users\branesh\sql-data-warehouse\datasets\source_crm\cust_info.csv'
	DELIMITER ','
	CSV HEADER;
	
	SELECT count(*) INTO crm_cust_count FROM bronze.crm_cust_info;
	RAISE NOTICE 'Loaded % rows into bronze.crm_cust_info', crm_cust_count;
	end_time := now();
	RAISE NOTICE '>> Load duration: % seconds', age(end_time,start_time);
    RAISE NOTICE '----------------------------------------';


    -- Load CRM Product Information
	start_time := now();
    RAISE NOTICE 'Loading CRM product information...';
	TRUNCATE TABLE bronze.crm_prd_info;
	
	COPY bronze.crm_prd_info(prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
	FROM 'C:\Users\branesh\sql-data-warehouse\datasets\source_crm\prd_info.csv'
	DELIMITER ','
	CSV HEADER;
	
	SELECT count(*) INTO crm_prd_count FROM bronze.crm_prd_info;
	RAISE NOTICE 'Loaded % rows into bronze.crm_prd_info', crm_prd_count;
    end_time := now();
	RAISE NOTICE '>> Load duration: % seconds', age(end_time,start_time);
	RAISE NOTICE '----------------------------------------';


    -- Load CRM Sales Details
	start_time := now();
    RAISE NOTICE 'Loading CRM sales details...';
	TRUNCATE TABLE bronze.crm_sales_details;
	
	COPY bronze.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
	FROM 'C:\Users\branesh\sql-data-warehouse\datasets\source_crm\sales_details.csv'
	DELIMITER ','
	CSV HEADER;
	
	SELECT count(*) INTO crm_sales_count FROM bronze.crm_sales_details;
	RAISE NOTICE 'Loaded % rows into bronze.crm_sales_details', crm_sales_count;
    end_time := now();
	RAISE NOTICE '>> Load duration: % seconds', age(end_time,start_time);
	RAISE NOTICE '----------------------------------------';

    
    -- Load ERP Customer Data (AZ12)
	start_time := now();
    RAISE NOTICE 'Loading ERP customer data (AZ12)...';
	TRUNCATE TABLE bronze.erp_cust_az12;
	
	COPY bronze.erp_cust_az12(CID,BDATE,GEN)
	FROM 'C:\Users\branesh\sql-data-warehouse\datasets\source_erp\CUST_AZ12.csv'
	DELIMITER ','
	CSV HEADER;
	
	SELECT count(*) INTO erp_cust_count FROM bronze.erp_cust_az12;
	RAISE NOTICE 'Loaded % rows into bronze.erp_cust_az12', erp_cust_count;
    end_time := now();
	RAISE NOTICE '>> Load duration: % seconds', age(end_time,start_time);
	RAISE NOTICE '----------------------------------------';


    -- Load ERP Location Data (A101)
	start_time := now();
    RAISE NOTICE 'Loading ERP location data (A101)...';
	TRUNCATE TABLE bronze.erp_loc_a101;
	
	COPY bronze.erp_loc_a101(CID,CNTRY)
	FROM 'C:\Users\branesh\sql-data-warehouse\datasets\source_erp\LOC_A101.csv'
	DELIMITER ','
	CSV HEADER;
	
	SELECT count(*) INTO erp_loc_count FROM bronze.erp_loc_a101;
	RAISE NOTICE 'Loaded % rows into bronze.erp_loc_a101', erp_loc_count;
    end_time := now();
	RAISE NOTICE '>> Load duration: % seconds', age(end_time,start_time);
	RAISE NOTICE '----------------------------------------';


    -- Load ERP Product Category Data (G1V2)
	start_time := now();
    RAISE NOTICE 'Loading ERP product category data (G1V2)...';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
	COPY bronze.erp_px_cat_g1v2(ID,CAT,SUBCAT,MAINTENANCE)
	FROM 'C:\Users\branesh\sql-data-warehouse\datasets\source_erp\PX_CAT_G1V2.csv'
	DELIMITER ','
	CSV HEADER;
	
	SELECT count(*) INTO erp_px_count FROM bronze.erp_px_cat_g1v2;
	RAISE NOTICE 'Loaded % rows into bronze.erp_px_cat_g1v2', erp_px_count;
	end_time := now();
	RAISE NOTICE '>> Load duration: % seconds', age(end_time,start_time);

	batch_end_time := now();
    RAISE NOTICE '======================================';
    RAISE NOTICE 'Summary - CRM: % customers, % products, % sales | ERP: % customers, % locations, % categories and 
				 TOTAL TIME TAKEN : % seconds', 
                 crm_cust_count, crm_prd_count, crm_sales_count, erp_cust_count, erp_loc_count, erp_px_count, age(batch_end_time,batch_start_time);
	RAISE NOTICE 'Bronze layer data load completed at %', NOW();

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in bronze data load: %', SQLERRM;
END;
$$;