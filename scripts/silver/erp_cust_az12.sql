/*
=============================================================
Load Script: silver.erp_cust_az12
=============================================================
Purpose:
	Populate the silver-layer ERP customer attributes from
	bronze, standardizing customer IDs, birthdates, and gender.

Logic summary:
	- Strips the 'NAS' prefix from cid when present.
	- Nulls birthdates that are in the future.
	- Normalizes gender values to 'Female', 'Male', or 'N/A'.

Source:
	- DataWarehouse.bronze.erp_cust_az12
Target:
	- silver.erp_cust_az12
*/

INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)
SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
	CASE
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
		ELSE 'N/A'
	END AS gen
FROM DataWarehouse.bronze.erp_cust_az12;

