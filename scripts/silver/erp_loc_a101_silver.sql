/****** Script for SelectTopNRows command from SSMS  ******/
insert into silver.erp_loc_a101(cid, cntry)
SELECT replace(cid, '-', '')cid
      ,case when trim([cntry]) = 'DE' then 'Germany'
	  when trim(cntry) in ('US', 'USA') then 'United States'
	  when trim(cntry) ='' or cntry is null then 'N/A'
	 else cntry
	 end cntry
  FROM [DataWarehouse].[bronze].[erp_loc_a101]
