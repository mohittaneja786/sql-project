/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


If Object_id ('silver.Crm_cust_info','U') IS NOT NULL
	DROP Table silver.Crm_cust_info;
	CREATE TABLE silver.Crm_cust_info (
		cst_id INT,
		cst_key NVARCHAR(50),
		cst_firstname NVARCHAR(50),
		cst_lastname NVARCHAR(50),
		cst_marital_status NVARCHAR(50),
		cst_gndr NVARCHAR(50),
		cst_create_date NVARCHAR(50),
		dwh_create_date DateTime2 Default Getdate()
)

SELECT 
CONVERT(DATE, cst_create_date, 105) AS cst_create_date
FROM silver.Crm_cust_info;



If Object_id ('silver.Crm_prd_info','U') IS NOT NULL
	DROP Table silver.Crm_prd_info;
	CREATE TABLE silver.Crm_prd_info (
		prd_id INT,
		prd_key NVARCHAR(50),
		prd_nm NVARCHAR(50),
		prd_cost INT,
		prd_line CHAR(10),
		prd_start NVARCHAR(50),
		prd_end_date NVARCHAR(50),
		dwh_create_date DateTime2 Default Getdate()
)


SELECT 
CONVERT(DATE, prd_start, 105) AS prd_start
--CONVERT(DATE, prd_end_date, 105) AS prd_end_date
FROM silver.Crm_prd_info;






If Object_id ('silver.Crm_sales_details','U') IS NOT NULL
	DROP Table silver.Crm_sales_details;
	CREATE TABLE silver.Crm_sales_details (
		sls_ord_num NVARCHAR(50),
		sls_prd_key NVARCHAR(50),
		sls_cust_id INT,
		sls_order_dt INT,
		sls_ship_dt INT,
		sls_due_dt INT,
		sls_sales INT,
		sls_quantity INT,
		sls_price INT,
		dwh_create_date DateTime2 Default Getdate()
)


If Object_id ('silver.erp_cust_az12','U') IS NOT NULL
	DROP Table silver.erp_cust_az12;
	CREATE TABLE silver.erp_cust_az12(
		cid NVARCHAR(50),
		bdate NVARCHAR(50),
		gen CHAR(15),
		dwh_create_date DateTime2 Default Getdate()
)


SELECT 
CONVERT(DATE, bdate, 105) AS bdate
FROM silver.erp_cust_az12;



If Object_id ('silver.erp_loc_a101','U') IS NOT NULL
	DROP Table silver.erp_loc_a101;
	CREATE TABLE silver.erp_loc_a101(
		cid NVARCHAR(50),
		cntry CHAR(50),
		dwh_create_date DateTime2 Default Getdate()
)



If Object_id ('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP Table silver.erp_px_cat_g1v2;
	CREATE TABLE silver.erp_px_cat_g1v2(
		id NVARCHAR(50),
		cat CHAR(50),
		subcat CHAR(50),
		maintenance CHAR(4),
		dwh_create_date DateTime2 Default Getdate()
)



SELECT *
FROM INFORMATION_SCHEMA.COLUMNS;

/*


SELECT T.TABLE_NAME, C.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLES T
Full Join INFORMATION_SCHEMA.COLUMNS C
ON C.TABLE_NAME = T.TABLE_NAME;

EXEC sp_rename 'silver.erp_cust_az12.erp_cid', 'cid', 'COLUMN';
EXEC sp_rename 'silver.erp_cust_az12.erp_bdate', 'bdate', 'COLUMN';
EXEC sp_rename 'silver.erp_cust_az12.erp_gen', 'gen', 'COLUMN';
*/
