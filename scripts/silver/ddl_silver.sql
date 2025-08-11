/*
------------------------------------------------------------------------
CREATE SILVER LAYER TABLES FOR DATA WAREHOUSE
------------------------------------------------------------------------
 SCRIPT PURPOSE:
 *	This script creates all silver layer tables for the medallion architecture.
 *	Silver layer contains cleaned, transformed, and business-ready data.
 *	Tables include data warehouse metadata columns for tracking and auditing.
 
 SILVER LAYER CHARACTERISTICS:
 *	Cleaned and validated data from bronze layer
 *	Standardized formats and business rules applied
 *	Deduplicated records with consistent data types
 *	Enhanced with derived columns and calculated fields
 *	Added DWH metadata columns for lineage tracking
 
 TABLES CREATED:
 *	CRM Tables: crm_cust_info, crm_prd_info, crm_sales_details
 *	ERP Tables: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2
 
 WARNING:
 *	Running this script will drop all existing silver tables if they exist.
 *	Ensure you have proper backups before executing this script.
 *	Silver tables depend on bronze layer - ensure bronze schema exists.
*/

--drop table if exists already
DROP TABLE IF EXISTS silver.crm_cust_info;

--create new table 
CREATE TABLE silver.crm_cust_info(
cst_id int,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(50),
cst_gndr varchar(50),
cst_create_date date,
dwh_create_date timestamp DEFAULT now() --added metadata column
);


--drop table if exists already
DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info(
prd_id int,
cat_id varchar(50),
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date timestamp DEFAULT now() --added metadata column
);


--drop table if exists already
DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int ,
sls_due_dt int ,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date timestamp DEFAULT now() --added metadata column
);


--drop table if exists already
DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12(
CID varchar(50),
BDATE date ,
GEN varchar(50),
dwh_create_date timestamp DEFAULT now() --added metadata column
);


--drop table if exists already
DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101(
CID varchar(50),
CNTRY varchar(50),
dwh_create_date timestamp DEFAULT now() --added metadata column
);


--drop table if exists already
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50),
dwh_create_date timestamp DEFAULT now() --added metadata column
);
