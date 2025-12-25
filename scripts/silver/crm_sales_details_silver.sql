/*
=============================================================
Load Script: silver.crm_sales_details
=============================================================
Purpose:
    Populate the silver-layer sales details table from bronze,
    converting dates and repairing inconsistent sales/price
    values.

Logic summary:
    - Converts integer date fields to DATE, setting invalid
      or malformed values to NULL.
    - Ensures sls_sales equals quantity * ABS(price) when the
      original value is missing, non-positive, or inconsistent.
    - Derives price when missing or non-positive as
      sls_sales / quantity (with NULLIF safeguard).
    - Uses DISTINCT to avoid duplicate order lines.

Source:
    - DataWarehouse.bronze.crm_sales_details
Target:
    - silver.crm_sales_details
*/

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
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE
        WHEN sls_sales IS NULL
             OR sls_sales <= 0
             OR sls_sales <> sls_quantity * ABS(sls_price)
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