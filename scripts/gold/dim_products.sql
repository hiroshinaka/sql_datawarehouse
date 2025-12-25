/*
=============================================================
View: gold.dim_products
=============================================================
Purpose:
    Product dimension enriched with category and maintenance
    information from ERP.

Grain:
    One row per current product (prd_key) where prd_end_dt IS NULL.

Sources:
    - DataWarehouse.silver.crm_prd_info   (base product data)
    - DataWarehouse.silver.erp_px_cat_g1v2 (category attributes)
*/

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM DataWarehouse.silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;


