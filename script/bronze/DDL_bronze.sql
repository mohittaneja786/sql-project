/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

If Object_id ('bronze.Crm_cust_info','U') IS NOT NULL
	DROP Table bronze.Crm_cust_info;
CREATE TABLE bronze.Crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date NVARCHAR(50)
)

SELECT 
CONVERT(DATE, cst_create_date, 105) AS cst_create_date
FROM bronze.Crm_cust_info;



If Object_id ('bronze.Crm_prd_info','U') IS NOT NULL
	DROP Table bronze.Crm_prd_info;
CREATE TABLE bronze.Crm_prd_info (
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line CHAR(10),
prd_start NVARCHAR(50),
prd_end_date NVARCHAR(50)
)


SELECT 
--CONVERT(DATE, prd_start, 105) AS prd_start
CONVERT(DATE, prd_end_date, 105) AS prd_end_date
FROM bronze.Crm_prd_info;


If Object_id ('bronze.Crm_sales_details','U') IS NOT NULL
	DROP Table bronze.Crm_sales_details;
CREATE TABLE bronze.Crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
)


If Object_id ('bronze.erp_cust_az12','U') IS NOT NULL
	DROP Table bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate NVARCHAR(50),
gen CHAR(15)
)


SELECT 
CONVERT(DATE, bdate, 105) AS bdate
FROM bronze.erp_cust_az12;



If Object_id ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP Table bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry CHAR(50)
)



If Object_id ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP Table bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat CHAR(50),
subcat CHAR(50),
maintenance CHAR(4)
)




SELECT T.TABLE_NAME, C.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLES T
Full Join INFORMATION_SCHEMA.COLUMNS C
ON C.TABLE_NAME = T.TABLE_NAME;


SELECT *
FROM INFORMATION_SCHEMA.COLUMNS;

/*
EXEC sp_rename 'bronze.erp_cust_az12.erp_cid', 'cid', 'COLUMN';
EXEC sp_rename 'bronze.erp_cust_az12.erp_bdate', 'bdate', 'COLUMN';
EXEC sp_rename 'bronze.erp_cust_az12.erp_gen', 'gen', 'COLUMN';



EXEC sp_rename 'bronze.erp_px_cat_g1v2.erp_id', 'id', 'COLUMN';
EXEC sp_rename 'bronze.erp_px_cat_g1v2.erp_cat', 'cat', 'COLUMN';
EXEC sp_rename 'bronze.erp_px_cat_g1v2.erp_subcat', 'subcat', 'COLUMN';
EXEC sp_rename 'bronze.erp_px_cat_g1v2.erp_maintenance', 'maintenance', 'COLUMN';


EXEC sp_rename 'bronze.erp_loc_a101.erp_cid', 'cid', 'COLUMN';
EXEC sp_rename 'bronze.erp_loc_a101.erp_cntry', 'cntry', 'COLUMN';
*/
