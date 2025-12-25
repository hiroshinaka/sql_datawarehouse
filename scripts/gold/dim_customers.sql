/*
=============================================================
View: gold.dim_customers
=============================================================
Purpose:
   Customer dimension enriched with demographics and location
   from ERP sources.

Grain:
   One row per unique customer_id (cst_id).

Sources:
   - DataWarehouse.silver.crm_cust_info  (base customer data)
   - DataWarehouse.silver.erp_cust_az12  (gender, birthdate)
   - DataWarehouse.silver.erp_loc_a101   (country)
*/

CREATE VIEW [gold].[dim_customers] AS
SELECT
   ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
   ci.cst_id         AS customer_id,
   ci.cst_key        AS customer_number,
   ci.cst_firstname  AS first_name,
   ci.cst_lastname   AS last_name,
   ci.cst_martial_status AS marital_status,
   CASE
      WHEN ci.cst_gndr <> 'N/A' THEN ci.cst_gndr
      ELSE COALESCE(ca.gen, 'N/A')
   END AS gender,
   ca.bdate          AS birthdate,
   la.cntry          AS country,
   ci.cst_create_date AS create_date
FROM [DataWarehouse].[silver].[crm_cust_info] AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
   ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
   ON ci.cst_key = la.cid;
