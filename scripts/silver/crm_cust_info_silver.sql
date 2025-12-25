/*
=============================================================
Load Script: silver.crm_cust_info
=============================================================
Purpose:
	Populate the silver-layer customer table from bronze,
	applying basic data cleansing and deduplication.

Logic summary:
	- Trims first and last names.
	- Normalizes marital status codes (S/M → Single/Married, else N/A).
	- Normalizes gender codes (F/M → Female/Male, else N/A).
	- Keeps only the latest record per cst_id using ROW_NUMBER().

Source:
	- bronze.crm_cust_info
Target:
	- silver.crm_cust_info
*/

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_martial_status,
	cst_gndr,
	cst_create_date
)
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname)  AS cst_lastname,
	CASE
		WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
		ELSE 'N/A'
	END AS cst_martial_status,
	CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'N/A'
	END AS cst_gndr,
	cst_create_date
FROM (
	SELECT *,
		   ROW_NUMBER() OVER (
			   PARTITION BY cst_id
			   ORDER BY cst_create_date DESC
		   ) AS flag_last
	FROM bronze.crm_cust_info
) AS t
WHERE t.flag_last = 1;

