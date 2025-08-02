/*CREATE DATABASE AND SCHEMAS
 * SCRIPT PURPOSE:
 * This script will check if db named "DataWarehouse" exists or not.
 * If exists use Drop command Else Create command
 
 
 *Additionally it creates three schemas for Medallion Architecture.*/


--create database if not exists
SELECT
	datname
FROM
	pg_database
WHERE
	datname = 'DataWarehouse';

DROP DATABASE IF EXISTS "DataWarehouse";

--create database
CREATE DATABASE "DataWarehouse";

--create schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;
