insert into silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT [id]
      ,[cat]
      ,[subcat]
      ,[maintenance]
  FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]