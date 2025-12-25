create or alter procedure silver.load_silver as 
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try 
		set @batch_start_time = GETDATE();
		print'======================'
		print'Loading Silver Layer'
		print'======================'

		print'======================'
		print'Loading CRM data'
		print'======================'

		SET @start_time = getdate()
		print 'Truncating silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		print 'Inserting into silver.crm_cust_info';

		INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_martial_status, cst_gndr, cst_create_date)
			SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
				ELSE 'N/A'
				END cst_martial_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'N/A'
				END cst_gndr,
			cst_create_date
			FROM (
			SELECT *, 
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
			FROM bronze.crm_cust_info
			)t WHERE flag_last = 1
		set @end_time = getdate() 
		print 'Load to silver.crm_cust_info complete';
		print '>> Load duration for silver.crm_prd_info: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print'------------------------'


		SET @start_time = getdate()
		print 'Truncating silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print 'Inserting into silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info (
			[prd_id],
			[cat_id],
			[prd_key],
			[prd_nm],
			[prd_cost],
			[prd_line],
			[prd_start_dt],
			[prd_end_dt]
		)
		SELECT [prd_id]
			  ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_' ) AS cat_id
			  ,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
			  ,[prd_nm]
			  ,ISNULL([prd_cost], 0) AS prd_cost
			  ,CASE UPPER(TRIM(prd_line)) 
			  WHEN 'M' THEN 'Mountatin'
			  WHEN 'R' THEN 'Road'
			  WHEN 'S' THEN 'other Sales'
			  WHEN 'T' THEN 'Touring'
			  ELSE 'N/A'
			  END AS prd_line
			  ,prd_start_dt AS [prd_start_dt]
			  ,DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS [prd_end_dt]
		  FROM [DataWarehouse].[bronze].[crm_prd_info]

		set @end_time = getdate() 
		print 'Load to silver.crm_prd_info complete';
		print '>> Load duration for silver.crm_prd_info: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print'------------------------'

		SET @start_time = getdate()
		print 'Truncating silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print 'Inserting into silver.crm_sales_details';

		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT DISTINCT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales IS NULL
					 OR sls_sales <= 0
					 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
		FROM DataWarehouse.bronze.crm_sales_details;
		set @end_time = getdate() 
		print 'Load to silver.crm_sales_details complete';
		print '>> Load duration for bronze.crm_sales_details: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'




		print'======================'
		print'Loading ERP data'
		print'======================'

		SET @start_time = getdate()
		print 'Truncating silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;
		print 'Inserting into silver.erp_cust_az12';
		insert into silver.erp_cust_az12(cid, bdate, gen)
		SELECT  
				case when [cid] like 'NAS%' then substring(cid, 4, len(cid))
				else cid 
				end cid
			  ,case when bdate > getdate() then null 
			  else bdate
			  end bdate
			  ,case when upper(trim(gen)) in ('F', 'Female') then 'Female'
			  when upper(trim(gen)) in ('M', 'Male') then 'Male'
			  else 'N/A'
			  end gen
		  FROM [DataWarehouse].[bronze].[erp_cust_az12]
  
  		set @end_time = getdate() 
		print 'Load to silver.erp_cust_az12 complete';
		print '>> Load duration for silver.erp_cust_az12: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print'------------------------'


		SET @start_time = getdate()
		print 'Truncating silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print 'Inserting into silver.erp_loc_a101';

		insert into silver.erp_loc_a101(cid, cntry)
		SELECT replace(cid, '-', '')cid
			  ,case when trim([cntry]) = 'DE' then 'Germany'
			  when trim(cntry) in ('US', 'USA') then 'United States'
			  when trim(cntry) ='' or cntry is null then 'N/A'
			 else cntry
			 end cntry
		  FROM [DataWarehouse].[bronze].[erp_loc_a101]

		set @end_time = getdate()
		print 'Load to silver.erp_loc_a101 complete';
		print '>> Load duration for silver.erp_loc_a101: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print'------------------------'

		SET @start_time = getdate()
		print 'Truncating silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		print 'Inserting into silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
		SELECT [id]
			  ,[cat]
			  ,[subcat]
			  ,[maintenance]
		  FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]
		set @end_time = getdate() 
		print '>> Load duration for silver.erp_px_cat_g1v2: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print 'Load to silver.erp_px_cat_g1v2 complete';
		print'------------------------'

		set @batch_end_time = GETDATE();



		print'======================'
		print'COMPLETE'
		print'Process time: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds'
		print'======================'	
	end try
	begin catch
		print'======================'
		print' Error occurred during silver layer loading'
		print 'Error Message' + ERROR_MESSAGE();
		print '======================'
	end catch
end