/*
=============================================================
Load Script: silver.erp_px_cat_g1v2
=============================================================
Purpose:
    Copy ERP product category metadata from bronze to silver
    without transformation.

Logic summary:
    - Straight insert-select of id, category, subcategory, and
      maintenance attributes.

Source:
    - DataWarehouse.bronze.erp_px_cat_g1v2
Target:
    - silver.erp_px_cat_g1v2
*/

INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM DataWarehouse.bronze.erp_px_cat_g1v2;