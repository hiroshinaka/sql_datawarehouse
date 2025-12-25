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
  
