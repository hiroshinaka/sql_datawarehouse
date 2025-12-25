/*
=============================================================
Load Script: silver.erp_loc_a101
=============================================================
Purpose:
	Populate the silver-layer ERP location table from bronze,
	standardizing customer IDs and country values.

Logic summary:
	- Removes dashes from cid.
	- Maps country codes:
		* 'DE' → 'Germany'
		* 'US'/'USA' → 'United States'
	- Replaces blank or NULL cntry with 'N/A'.

Source:
	- DataWarehouse.bronze.erp_loc_a101
Target:
	- silver.erp_loc_a101
*/

INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
)
SELECT
	REPLACE(cid, '-', '') AS cid,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
		ELSE cntry
	END AS cntry
FROM DataWarehouse.bronze.erp_loc_a101;
