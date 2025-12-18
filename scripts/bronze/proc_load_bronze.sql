/*
=============================================================
Create Stored Procedure to Load Bronze Tables
=============================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema within the 'DataWarehouse' database.
    It populates the 'crm_cust_info', 'crm_prd_info', 'crm_sales_details', 'erp_px_cat_g1v2', 
    'erp_loc_a101', and 'erp_cust_az12' tables from corresponding CSV files.

WARNING:
    Running this stored procedure will drop and recreate the tables in the 'bronze' schema. 
    All data in the tables will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this stored procedure.
*/

create or alter procedure bronze.load_bronze as 
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try 
		set @batch_start_time = GETDATE();
		print'======================'
		print'Loading Bronze Layer'
		print'======================'

		print'======================'
		print'Loading CRM data'
		print'======================'

		SET @start_time = getdate()
		truncate table bronze.crm_cust_info
		bulk insert bronze.crm_cust_info
		from 'C:\Users\hiros\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		)
		set @end_time = getdate() 
		print '>> Load duration for bronze.crm_cust_info: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print'------------------------'
		SET @start_time = getdate()
		truncate table bronze.crm_prd_info
		bulk insert bronze.crm_prd_info
		from 'C:\Users\hiros\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		)
		set @end_time = getdate() 
		print '>> Load duration for bronze.crm_prd_info: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print'------------------------'

		SET @start_time = getdate()
		truncate table bronze.crm_sales_details
		bulk insert bronze.crm_sales_details
		from 'C:\Users\hiros\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		)
		set @end_time = getdate() 
		print '>> Load duration for bronze.crm_sales_details: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'

		print'======================'
		print'Loading ERP data'
		print'======================'


		SET @start_time = getdate()
		truncate table bronze.erp_cust_az12
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\hiros\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		)
		set @end_time = getdate() 
		print '>> Load duration for bronze.erp_cust_az12: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print'------------------------'

		SET @start_time = getdate()
		truncate table bronze.erp_loc_a101
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\hiros\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		)
		set @end_time = getdate() 
		print '>> Load duration for bronze.erp_loc_a101: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print'------------------------'

		SET @start_time = getdate()
		truncate table bronze.erp_px_cat_g1v2
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\hiros\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		)
		set @end_time = getdate() 
		print '>> Load duration for bronze.erp_px_cat_g1v2: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		set @batch_end_time = GETDATE();



		print'======================'
		print'COMPLETE'
		print'Process time: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds'
		print'======================'

	end try
	begin catch
		print'======================'
		print' Error occurred during bronze layer loading'
		print 'Error Message' + ERROR_MESSAGE();
		print '======================'
	end catch
end


exec bronze.load_bronze