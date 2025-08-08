/*
------------------------------------------------------------------------
CREATE BRONZE LAYER TABLES FOR DATA WAREHOUSE
------------------------------------------------------------------------
 SCRIPT PURPOSE:
 *	This script creates all bronze layer tables for the medallion architecture.
 *	It includes tables for both CRM and ERP source systems.
 *	Bronze layer stores raw data in its original format from source systems.
 
 TABLES CREATED:
 *	CRM Tables: crm_cust_info, crm_prd_info, crm_sales_details
 *	ERP Tables: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2
 
 WARNING:
 *	Running this script will drop all existing bronze tables if they exist.
 *	Ensure you have proper backups before executing this script.
*/

--drop table if exists already
DROP TABLE IF EXISTS bronze.crm_cust_info;

--create new table 
CREATE TABLE bronze.crm_cust_info(
cst_id int,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(50),
cst_gndr varchar(50),
cst_create_date date
);


--drop table if exists already
DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
prd_id int,
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date
);


--drop table if exists already
DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int ,
sls_due_dt int ,
sls_sales int,
sls_quantity int,
sls_price int
);


--drop table if exists already
DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(
CID varchar(50),
BDATE date ,
GEN varchar(50)
);


--drop table if exists already
DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(
CID varchar(50),
CNTRY varchar(50)
);


--drop table if exists already
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50)
);
