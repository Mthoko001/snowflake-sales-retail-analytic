USE WAREHOUSE sales_etl_wh;
USE DATABASE sales_db;
USE SCHEMA silver;

``
--Check Bronz_Customer_Master
-- bad records track in PK,
SELECT *
FROM sales_db.bronze.bronze_customer_master
WHERE customer_id IS NULL
   OR TRIM(customer_id) = ''
   OR NOT REGEXP_LIKE(
        UPPER(TRIM(customer_id)),
        '^[A-Z]{3,4}-[0-9]+$'
     );

     --check valid but duplicate PK
  SELECT
    TRIM(customer_id)              AS customer_id,
    LISTAGG(customer_id, ' | ')    AS raw_customer_ids,
    LISTAGG(full_name, ' | ')      AS raw_full_names,
    COUNT(*)                       AS record_count
FROM sales_db.bronze.bronze_customer_master
WHERE customer_id IS NOT NULL
  AND TRIM(customer_id) <> ''
  AND customer_id <> '#N/A'
  AND REGEXP_LIKE(
        UPPER(TRIM(customer_id)),
        '^[A-Z]{3,4}-[0-9]+$'
      )
GROUP BY TRIM(customer_id)
HAVING COUNT(*) > 1;


--check sales_db.bronze.bronze_employee_master

--- check invalid ids
SELECT *
FROM sales_db.bronze.bronze_employee_master
WHERE employee_id IS NULL
   OR TRIM(employee_id) = ''
   OR employee_id = '#N/A'
   OR NOT REGEXP_LIKE(
        UPPER(TRIM(employee_id)),
        '^EMP-[0-9]{4}$'
     );

     -- check duplicate id

     SELECT
    TRIM(employee_id)               AS employee_id,
    LISTAGG(employee_id, ' | ')     AS raw_employee_ids,
    LISTAGG(full_name, ' | ')       AS raw_full_names,
    LISTAGG(store_id, ' | ')        AS raw_store_ids,
    COUNT(*)                        AS record_count
FROM sales_db.bronze.bronze_employee_master
WHERE employee_id IS NOT NULL
  AND TRIM(employee_id) <> ''
GROUP BY TRIM(employee_id)
HAVING COUNT(*) > 1;

--chec  inventory table
-- check null id
SELECT *
FROM sales_db.bronze.bronze_inventory_levels
WHERE inventory_id IS NULL
   OR TRIM(inventory_id) = '';

-- check invalid id format
SELECT *
FROM sales_db.bronze.bronze_inventory_levels
WHERE NOT REGEXP_LIKE(
    UPPER(TRIM(inventory_id)),
    '^INV-[0-9]+$'
);

--check duplicate id SELECT
SELECT
    TRIM(inventory_id) AS inventory_id,
    LISTAGG(inventory_id, ' | ') AS raw_inventory_ids,
    COUNT(*) AS record_count
FROM sales_db.bronze.bronze_inventory_levels
WHERE inventory_id IS NOT NULL
  AND TRIM(inventory_id) <> ''
GROUP BY TRIM(inventory_id)
HAVING COUNT(*) > 1;


SELECT inventory_id, COUNT(*)
FROM silver.inventory_levels
GROUP BY inventory_id
HAVING COUNT(*) > 1;

SELECT *
FROM silver.inventory_levels
WHERE inventory_id IS NULL;

-- check order lines table

-- check null or empy id
SELECT *
FROM sales_db.bronze.bronze_order_lines
WHERE order_line_id IS NULL
   OR TRIM(order_line_id) = '';

   -- check invalid orderline format
   SELECT *
FROM sales_db.bronze.bronze_order_lines
WHERE NOT REGEXP_LIKE(
    UPPER(TRIM(order_line_id)),
    '^OL-[0-9]+$'
);

-- check duplicate id
SELECT
    TRIM(order_line_id) AS order_line_id,
    LISTAGG(order_line_id, ' | ') AS raw_order_line_ids,
    COUNT(*) AS record_count
FROM sales_db.bronze.bronze_order_lines
WHERE order_line_id IS NOT NULL
  AND TRIM(order_line_id) <> ''
GROUP BY TRIM(order_line_id)
HAVING COUNT(*) > 1;

select
count(*) as total_rows,
count(distinct trim(order_line_id)) as dist_order_is
from sales_db.bronze.bronze_order_lines;

-- check product master table

--cheking null or empty id--

SELECT *
FROM sales_db.bronze.bronze_product_master
WHERE id IS NULL
   OR TRIM(id) = '';

   -- check invalid id --
   SELECT *
FROM sales_db.bronze.bronze_product_master
WHERE NOT REGEXP_LIKE(
    UPPER(TRIM(id)),
    '^[A-Z][0-9]+$'
);

--check duplicate id --
SELECT
    TRIM(id) AS product_id,
    LISTAGG(id, ' | ')        AS raw_product_ids,
    LISTAGG(name, ' | ')      AS raw_names,
    LISTAGG(category, ' | ')  AS raw_categories,
    LISTAGG(unit_price, ' | ') AS raw_unit_prices,
    COUNT(*) AS record_count
FROM sales_db.bronze.bronze_product_master
WHERE id IS NOT NULL
  AND TRIM(id) <> ''
GROUP BY TRIM(id)
HAVING COUNT(*) > 1;

--checking invalid price --
SELECT *
FROM sales_db.bronze.bronze_product_master
WHERE unit_price IS NULL
   OR TRIM(unit_price) = ''
   OR NOT REGEXP_LIKE(
        TRIM(unit_price),
        '^[0-9]+(\.[0-9]+)?$'
     );

-- health summary--
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT TRIM(id)) AS distinct_product_ids
FROM sales_db.bronze.bronze_product_master;


-- check sales target table --

-- check empty or null id --
SELECT *
FROM sales_db.bronze.bronze_sales_targets
WHERE target_id IS NULL
   OR TRIM(target_id) = '';

-- check invalid id --

SELECT *
FROM sales_db.bronze.bronze_sales_targets
WHERE NOT REGEXP_LIKE(
    UPPER(TRIM(target_id)),
    '^TGT-[0-9]+$'
);

--helath summary --

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT TRIM(target_id)) AS distinct_target_ids
FROM sales_db.bronze.bronze_sales_targets;

--check sales transactions --

--check empty or null PK--

SELECT *
FROM sales_db.bronze.bronze_sales_transactions
WHERE transaction_id IS NULL
   OR TRIM(transaction_id) = '';

-- checking invalid PK--
SELECT *
FROM sales_db.bronze.bronze_sales_transactions
WHERE NOT REGEXP_LIKE(
    UPPER(TRIM(transaction_id)),
    '^TRN-[0-9]+$'
);

-- check duplicate PK--
SELECT
    TRIM(transaction_id) AS transaction_id,
    COUNT(*) AS record_count
FROM sales_db.bronze.bronze_sales_transactions
WHERE transaction_id IS NOT NULL
  AND TRIM(transaction_id) <> ''
GROUP BY TRIM(transaction_id)
HAVING COUNT(*) > 1;