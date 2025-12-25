/*
=============================================================
View: gold.fact_sales
=============================================================
Purpose:
    Sales fact view at order line grain, joined to the
    product and customer dimensions.

Grain:
    One row per sales order line (sls_ord_num + product).

Sources:
    - DataWarehouse.silver.crm_sales_details (base fact data)
    - gold.dim_products                      (product dimension)
    - gold.dim_customers                     (customer dimension)


CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num   AS order_number,
    pr.product_key   AS product_key,
    cr.customer_key  AS customer_key,
    sd.sls_order_dt  AS order_date,
    sd.sls_ship_dt   AS ship_date,
    sd.sls_due_dt    AS due_date,
    sd.sls_sales     AS sales_amount,
    sd.sls_quantity  AS quantity,
    sd.sls_price     AS price
FROM DataWarehouse.silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cr
    ON sd.sls_cust_id = cr.customer_id;