
--- Before EXEC/initiate make sure change the excel path'

exec bronze.load_bronze

Create OR Alter Procedure bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DateTIme, @batch_end_time DATETIME;
	Begin TRY

	set @batch_end_time = GetDate();
		Print'=======================================================================================';
		Print 'Loading Bronze Layer';
		Print '======================================================================================';


			Print'-------------------------------------------------------------';
			Print 'Loading CRM Tables';
			Print'-------------------------------------------------------------';

			set @start_time = GETDATE();

			Print '>>Truncating Table: bronze.crm_cust_info';
			Truncate table bronze.crm_cust_info;
			Print '>>Inserting Data Into Table: bronze.crm_cust_info'
			BULK INSERT bronze.Crm_cust_info
			FROM "T:\DAAA\Code with Barrer\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
			WITH (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK 
			);
			
			set @END_time = GETDATE();
			PRINT'>> lOAD DURATION :' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
			PRINT'>>-------------------->>';

			--select count(*) from bronze.crm_cust_info;

			--select * from bronze.Crm_cust_info;

			-----------------------------------------------

			Print '>>------------------------------';
			Print '>>Truncating Table: bronze.crm_prd_info';
			Truncate table bronze.crm_prd_info;
			Print '>>Inserting Data into PRD_INFO csv ';

			set @start_time = GetDate();

				BULK INSERT bronze.Crm_prd_info
				FROM "T:\DAAA\Code with Barrer\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
				WITH (
				 FIRSTROW = 2,
				 FIELDTERMINATOR = ',',
				 TABLOCK 
				);
			set @end_time = GetDate();
			PRINT'>> lOAD DURATION :' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
			PRINT'>>-------------------->>';


			--select * from bronze.Crm_prd_info;


			-------------------------------------------------
			
			set @start_time = GetDate();

			Print'>> truncating the Table : Sales Details';
			Truncate table bronze.crm_sales_details;
			Print '>> Inserting data into Sales Detials';

				BULK INSERT bronze.Crm_sales_details
				FROM "T:\DAAA\Code with Barrer\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
				WITH (
				 FIRSTROW = 2,
				 FIELDTERMINATOR = ',',
				 TABLOCK 

				);
			set @end_time = GetDate();
			PRINT'>> lOAD DURATION :' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
			PRINT'>>-------------------->>';


			--select * from bronze.Crm_sales_details;

			-----------------------------------------------------------------

			Print'-------------------------------------------------------------';
			Print'Loading ERP Section';
	
			Print'-------------------------------------------------------------';
			Truncate table bronze.erp_cust_az12;

			BULK INSERT bronze.erp_cust_az12
			FROM 'T:\DAAA\Code with Barrer\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK 
			);

			--select count(*) from bronze.erp_cust_az12;
			--select * from bronze.erp_cust_az12;

			------------------------------------

			set @start_time = GetDate();

			Truncate table bronze.erp_loc_a101

			BULK INSERT bronze.erp_loc_a101
			FROM 'T:\DAAA\Code with Barrer\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK 
			);
			set @end_time = GetDate();
			PRINT'>> lOAD DURATION :' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
			PRINT'>>-------------------->>';

			--select count(*) from bronze.erp_loc_a101
			--select * from bronze.erp_loc_a101


			------------------------------------------------------

			set @start_time = GetDate();

			Truncate table bronze.erp_px_cat_g1v2

				BULK INSERT bronze.erp_px_cat_g1v2
				FROM 'T:\DAAA\Code with Barrer\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
				WITH (
				 FIRSTROW = 2,
				 FIELDTERMINATOR = ',',
				 TABLOCK 
				);

			set @end_time = GetDate();
			PRINT'>> lOAD DURATION :' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
			PRINT'>>---------------------------------------->>';


		set @batch_end_time = GetDate();
		PRINT '================================================';
		Print 'Loading Broze Layer is completed';
		PRINT'>> lOAD DURATION :' + cast(datediff(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'Seconds';
			PRINT'>>--------------------------------------->>';



	End Try

	Begin Catch
		Print '=================================================';
		Print '=====   ErroR OCCURED DURING LOADING BRONZE LAYER   ==============';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		Print '=================================================';
		
	END Catch

END

--select count(*) from bronze.erp_px_cat_g1v2
--select * from bronze.erp_px_cat_g1v2
