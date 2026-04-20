-- here i am creating views for easy reporting --
USE WAREHOUSE sales_etl_wh;
USE DATABASE sales_db;
USE SCHEMA gold;

-- create view for sales --
CREATE OR REPLACE VIEW gold.vw_sales_flat AS
SELECT
    /* Date */
    dd.date_key,
    dd.calendar_date,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,

    /* Customer */
    dc.customer_id,
    dc.full_name AS customer_name,

    /* Product */
    dp.product_id,
    dp.product_name,
    dp.category,

    /* Store */
    ds.store_id,
    ds.store_name,
    ds.city,
    ds.region,

    /* Employee */
    de.employee_id,
    de.full_name AS employee_name,

    /* Transaction identifiers */
    fs.transaction_id,
    fs.order_line_id,

    /* Measures */
    fs.quantity,
    fs.unit_price,
    fs.discount,
    fs.line_total

FROM gold.fact_sales fs
JOIN gold.dim_date dd
  ON fs.date_key = dd.date_key
LEFT JOIN gold.dim_customer dc
  ON fs.customer_key = dc.customer_key
LEFT JOIN gold.dim_product dp
  ON fs.product_key = dp.product_key
LEFT JOIN gold.dim_store ds
  ON fs.store_key = ds.store_key
LEFT JOIN gold.dim_employee de
  ON fs.employee_key = de.employee_key;

 -- create view for sales target --

 CREATE OR REPLACE VIEW gold.vw_sales_targets_flat AS
SELECT
    /* Date */
    dd.year,
    dd.month,
    dd.month_name,

    /* Employee */
    de.employee_id,
    de.full_name AS employee_name,

    /* Target */
    fst.target_id,
    fst.target_amount

FROM gold.fact_sales_targets fst
JOIN gold.dim_employee de
  ON fst.employee_key = de.employee_key
JOIN gold.dim_date dd
  ON fst.date_key = dd.date_key;

-- creating view for inventory snapshot --

CREATE OR REPLACE VIEW gold.vw_inventory_flat AS
SELECT
    /* Product */
    dp.product_id,
    dp.product_name,
    dp.category,

    /* Store */
    ds.store_id,
    ds.store_name,
    ds.city,
    ds.region,

    /* Inventory */
    fis.inventory_id,
    fis.quantity_on_hand,
    fis.snapshot_ts

FROM gold.fact_inventory_snapshot fis
JOIN gold.dim_product dp
  ON fis.product_key = dp.product_key
JOIN gold.dim_store ds
  ON fis.store_key = ds.store_key;