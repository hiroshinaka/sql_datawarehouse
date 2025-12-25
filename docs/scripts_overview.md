# SQL Data Warehouse Scripts Overview

This document describes the main database scripts in this project, their purpose, and a typical execution order for setting up and loading the warehouse.

## High-Level Layers

- **Bronze**: Raw landing tables loaded directly from source CSV files.
- **Silver**: Cleaned and conformed tables with business rules applied.
- **Gold**: Dimensional views (dimensions and fact tables) for analytics.

---

## Initialization

### scripts/init_db.sql
- Creates (or recreates) the `DataWarehouse` database.
- Drops the existing `DataWarehouse` if it exists (destructive operation).
- Creates three schemas: `bronze`, `silver`, and `gold`.
- **Use with caution**: all existing data in `DataWarehouse` will be lost when this script is run.

Typical usage:
1. Run once at environment setup or when you need to fully reset the warehouse.

---

## Bronze Layer

### scripts/bronze/ddl_bronze.sql
- Creates the raw **bronze** tables:
  - `bronze.crm_cust_info`
  - `bronze.crm_prd_info`
  - `bronze.crm_sales_details`
  - `bronze.erp_px_cat_g1v2`
  - `bronze.erp_loc_a101`
  - `bronze.erp_cust_az12`
- Drops and recreates these tables if they already exist (destructive to table data).

### scripts/bronze/proc_load_bronze.sql
- Defines `bronze.load_bronze` stored procedure.
- Truncates the bronze tables and bulk loads data from CSV files in the `datasets` folder (CRM and ERP sources).
- Prints simple timing and progress messages for each load step.
- At the bottom of the script, executes `bronze.load_bronze` once.

Typical bronze workflow:
1. Run `bronze/ddl_bronze.sql` to (re)create bronze tables.
2. Run `bronze/proc_load_bronze.sql` to load raw data into bronze.

---

## Silver Layer

### scripts/silver/ddl_silver.sql
- Creates the **silver** tables (cleaned layer) with `dwh_create_date` audit columns:
  - `silver.crm_cust_info`
  - `silver.crm_prd_info`
  - `silver.crm_sales_details`
  - `silver.erp_px_cat_g1v2`
  - `silver.erp_loc_a101`
  - `silver.erp_cust_az12`
- Drops and recreates these tables if they already exist (destructive to table data).

### scripts/silver/proc_load_silver.sql
- Defines `silver.load_silver` stored procedure.
- Implements transformations from **bronze** to **silver**:
  - **CRM customers**: trims names, normalizes marital status and gender codes, and keeps only the latest record per `cst_id`.
  - **CRM products**: derives `cat_id` and `prd_key` from `prd_key`, normalizes `prd_line`, fills missing costs, and maintains effective-dated ranges with `prd_start_dt` / `prd_end_dt`.
  - **CRM sales details**: converts integer dates to `DATE`, repairs invalid/missing sales amounts and prices.
  - **ERP customers (erp_cust_az12)**: cleans `cid`, fixes invalid future birthdates, normalizes gender values.
  - **ERP locations (erp_loc_a101)**: normalizes country codes to full names and handles missing values.
  - **ERP product categories (erp_px_cat_g1v2)**: copies category metadata into silver.
- For each table, truncates the silver table, inserts transformed data from the matching bronze table, and logs timings.

Typical silver workflow:
1. Ensure bronze tables are created and loaded.
2. Run `silver/ddl_silver.sql` to (re)create silver tables.
3. Run `silver/proc_load_silver.sql` and execute `silver.load_silver` to populate the silver layer.

---

## Gold Layer (Dimensional Views)

### scripts/gold/dim_customers.sql
- Creates the view `gold.dim_customers`.
- Purpose:
  - Builds a customer dimension at the grain of one row per `customer_id`.
  - Uses `ROW_NUMBER()` to generate a surrogate `customer_key`.
  - Pulls base customer attributes from `DataWarehouse.silver.crm_cust_info`.
  - Enriches with:
    - Gender and birthdate from `silver.erp_cust_az12`.
    - Country from `silver.erp_loc_a101`.
  - Fallback logic: if the CRM gender is `N/A`, uses the ERP gender, defaulting to `N/A` when missing.

### scripts/gold/dim_products.sql
- Creates the view `gold.dim_products`.
- Purpose:
  - Builds a product dimension with a surrogate `product_key` using `ROW_NUMBER()`.
  - Uses `DataWarehouse.silver.crm_prd_info` as the base.
  - Joins to `silver.erp_px_cat_g1v2` for category, subcategory, and maintenance attributes.
  - Filters to current products where `prd_end_dt IS NULL`.

### scripts/gold/fact__sales.sql
- Creates the view `gold.fact_sales`.
- Purpose:
  - Builds a sales fact table at order line grain.
  - Sources from `DataWarehouse.silver.crm_sales_details`.
  - Joins to `gold.dim_products` on product number to get `product_key`.
  - Joins to `gold.dim_customers` on customer id to get `customer_key`.
  - Exposes order, shipment, due dates and numeric measures such as sales amount, quantity, and price.

---

## Recommended End-to-End Run Order

1. `scripts/init_db.sql` – create/reset the `DataWarehouse` database and schemas.
2. `scripts/bronze/ddl_bronze.sql` – create bronze tables.
3. `scripts/silver/ddl_silver.sql` – create silver tables.
4. `scripts/bronze/proc_load_bronze.sql` – load raw data into bronze.
5. `scripts/silver/proc_load_silver.sql` – load and transform data into silver.
6. `scripts/gold/dim_customers.sql` – create customer dimension view.
7. `scripts/gold/dim_products.sql` – create product dimension view.
8. `scripts/gold/fact__sales.sql` – create sales fact view.

Use this sequence when setting up a fresh environment or re-running the full pipeline. For day-to-day operations, you typically re-run only the bronze and silver load procedures and keep the gold views in place.
