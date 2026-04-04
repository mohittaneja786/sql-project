exec silver.load_silver

Create or Alter procedure Silver.load_silver As

	Begin

	Declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	Begin try
	set @batch_start_time = getdate();
	
	----------------------------------------------------------------------
				  --Inserting into Silver.Crm_cust_info
	----------------------------------------------------------------------
	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	Print '>>>>>>>>>>>>  silver.Crm_cust_info>>>>>>>>>'

	Truncate Table silver.Crm_cust_info
	Insert into silver.Crm_cust_info 
	(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date )

	select 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			Trim(cst_lastname) AS cst_lastname,

			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
				When UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
				Else 'n/a'
			END cst_status,

			CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
				When UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
				Else 'n/a'
			END cst_gndr,

			cst_create_date
		from(
			select *,
		Row_Number () Over ( Partition by cst_id order by cst_create_date DESC) as Flag_last
	from bronze.crm_cust_info
	WHERE cst_id IS NOT NULL)t
	where Flag_last = 1


	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	Print '                                           '

	----------------------------------------------------------------------
				--Inserting into Silver.Crm_prd_info
	----------------------------------------------------------------------
	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	Print '>>>>>>>>>>>>  silver.Crm_prd_info>>>>>>>>>'


	Truncate Table silver.Crm_prd_info
	Insert into silver.Crm_prd_info
		(
			prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start ,
			prd_end_date	
	)
	select
	   prd_id,
	   Replace(Substring(prd_key, 1, 5 ), '-', '_' ) As cat_id,         -- Extract category ID from prd_key
	   Substring(prd_key, 7, len(prd_key) ) As prd_key,                 -- Extract category ID from prd_key
	   prd_nm,
	   isnull(prd_cost,0) As prd_cost,                                  -- NUll values converted into 0                          
   
	   Case upper(Trim(prd_line))
					When 'M' then 'Mountain'
					When 'R' then 'Road'
					When 'S' then 'Sales'
					When 'T' then 'Touring'
			   Else 'N/A'
			  end as prd_line,              -- Map product line codes to desciptive values

	  cast(prd_start As Date) As prd_start,
	   --cast(prd_end_date As Date) As prd_end,

	DATEADD(day,-1,
		LEAD(CAST(prd_start AS DATE)) 
		OVER (PARTITION BY prd_key ORDER BY prd_start)
	) AS prd_end                            -- Calculate the end date as one day before the next start date
	from bronze.Crm_prd_info;


	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	Print '                                           '

	----------------------------------------------------------------------
				 --Inserting into silver.Crm_sales_details
	----------------------------------------------------------------------
	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	Print '>>>>>>>>>>>>  silver.Crm_Sales_Details >>>>>>>>>'


	Truncate Table silver.Crm_sales_details
	INSERT INTO silver.Crm_sales_details (
		   sls_ord_num 
		  ,sls_prd_key 
		  ,sls_cust_id ,
		  sls_order_dt ,
		  sls_ship_dt ,
		  sls_due_dt ,
		  sls_sales ,
		  sls_quantity ,
		  sls_price 
	)
	SELECT
		   sls_ord_num
		  ,sls_prd_key
		  ,sls_cust_id
		  ,CASE When sls_order_dt = 0 OR LEN(sls_order_dt) != 8 Then Null
			   Else CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
		   END AS sls_order_dt

		  ,CASE When sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 Then Null
			   Else CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
		   END AS sls_ship_dt

		  ,CASE When sls_due_dt = 0 OR LEN(sls_due_dt) != 8 Then Null
			   Else CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
		   END AS sls_due_dt

		 ,CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
			Then sls_quantity * ABS(sls_price)
			Else sls_sales
			END AS sls_sales,

			sls_quantity

		,CASE WHEN sls_price IS NULL OR sls_price <=0
			THEN sls_sales / NULLIF(sls_quantity, 0)
			Else sls_price
		END AS sls_price
	  FROM bronze.Crm_sales_details
  

	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	Print '                                           '

	----------------------------------------------------------------------
				 --Inserting into silver.erp_cust_az12
	----------------------------------------------------------------------
	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	Print '>>>>>>>>>>>>  silver.erp_cust_az12 >>>>>>>>>'


	Truncate Table silver.erp_cust_az12
	INSERT INTO silver.erp_cust_az12 (
		cid, bdate, gen
	)
	select 
		CASE WHEN cid LIKE 'NAS%' Then substring(cid, 4, len(cid))
			ELSE cid
		END As cid,
	
		CASE When bdate > '2026-04-02' Then NULL
			Else bdate
		END bdate,
	
		CASE WHEN TRIM(UPPER(gen)) IN ('F', 'FEMALE') THEN 'Female' 
			 WHEN TRIM(UPPER(gen)) IN ('M', 'MALE') THEN 'Male' 
			 Else 'n/a'
		END as gen
	from bronze.erp_cust_az12

	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	Print '                                           '

	----------------------------------------------------------------------
				--Inserting into silver.erp_loc_a101
	----------------------------------------------------------------------
	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	Print '>>>>>>>>>>>>  silver.erp_loc_a101>>>>>>>>>'


	Truncate Table silver.erp_loc_a101
	Insert into silver.erp_loc_a101 (cid, cntry)
	select 

			Replace(cid, '-','') As cid,		
		
			case when TRIM(Upper(cntry)) IN ('US','USA') THen 'United States'
				when TRIM(Upper(cntry)) IN ('DE') Then 'Germany'
				when Trim(cntry) = '' OR cntry IS NULL Then 'n/a'
			else Trim(cntry)
			end cntry

	from bronze.erp_loc_a101

	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	Print '                                           '
	----------------------------------------------------------------------
			  --Inserting data into Silver.erp_px_cat_g1v2
	----------------------------------------------------------------------
	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	Print '>>>>>>>>>>>>  silver.erp_px_cat_g1v2 >>>>>>>>>'


	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	select
	 id,
	 cat,
	 subcat,
	 maintenance
	FROM bronze.erp_px_cat_g1v2;

	Print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	END TRY
	Begin catch
		print 'Error message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		print 'Error message' + CAST(ERROR_STATE() AS NVARCHAR);
	End Catch
END
