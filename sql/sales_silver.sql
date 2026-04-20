USE WAREHOUSE sales_etl_wh;
USE DATABASE sales_db;
USE SCHEMA silver;

-- create customer
CREATE OR REPLACE TABLE customer (
    customer_id VARCHAR NOT NULL,
    full_name   VARCHAR,
    load_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
``
--insert to silver layer,for valid id'd but duplicate,filter result after window function then choose first row 

INSERT INTO sales_db.silver.customer (customer_id, full_name)
SELECT
    TRIM(customer_id),
    TRIM(full_name)
FROM sales_db.bronze.bronze_customer_master
WHERE customer_id IS NOT NULL
  AND TRIM(customer_id) <> ''
  AND customer_id <> '#N/A'
  AND REGEXP_LIKE(
        UPPER(TRIM(customer_id)),
        '^[A-Z]{3,4}-[0-9]+$'
      )
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(customer_id)
    ORDER BY customer_id
) = 1;


--create employee table
CREATE OR REPLACE TABLE employee (
    employee_id VARCHAR NOT NULL,
    full_name   VARCHAR,
    store_id    VARCHAR,
    load_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- inser into employee

INSERT INTO silver.employee (
    employee_id,
    full_name,
    store_id
)
SELECT
    TRIM(employee_id) AS employee_id,
    TRIM(full_name)   AS full_name,
    TRIM(store_id)    AS store_id
FROM sales_db.bronze.bronze_employee_master
WHERE employee_id IS NOT NULL
  AND TRIM(employee_id) <> ''
  AND employee_id <> '#N/A'
  AND REGEXP_LIKE(
        UPPER(TRIM(employee_id)),
        '^EMP-[0-9]{4}$'
      )
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(employee_id)
    ORDER BY employee_id
) = 1;
``
--create inventory table

CREATE OR REPLACE TABLE inventory_levels (
    inventory_id      VARCHAR NOT NULL,
    product_id        VARCHAR,
    store_id          VARCHAR,
    quantity_on_hand  NUMBER(10,0),
    load_ts           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- insert inventory table

INSERT INTO silver.inventory_levels (
    inventory_id,
    product_id,
    store_id,
    quantity_on_hand
)
SELECT
    TRIM(inventory_id)        AS inventory_id,
    TRIM(product_id)          AS product_id,
    NULLIF(TRIM(store_id), '') AS store_id,
    TRY_TO_NUMBER(TRIM(quantity_on_hand)) AS quantity_on_hand
FROM sales_db.bronze.bronze_inventory_levels
WHERE inventory_id IS NOT NULL
  AND TRIM(inventory_id) <> ''
  AND inventory_id <> '#N/A'
  AND REGEXP_LIKE(
        UPPER(TRIM(inventory_id)),
        '^INV-[0-9]+$'
      )

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(inventory_id)
    ORDER BY
        -- prefer rows with valid store_id and quantity
        CASE WHEN TRIM(store_id) IS NOT NULL AND TRIM(store_id) <> '' THEN 1 ELSE 2 END,
        CASE WHEN TRY_TO_NUMBER(quantity_on_hand) IS NOT NULL THEN 1 ELSE 2 END
) = 1;

-- create table orderlines

CREATE OR REPLACE TABLE order_lines (
    order_line_id   VARCHAR NOT NULL,
    transaction_id  VARCHAR,
    product_id      VARCHAR,
    quantity         NUMBER(10,0),
    unit_price       NUMBER(10,2),
    discount         NUMBER(10,2),
    line_total       NUMBER(10,2),
    load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- insrting to table order lines
INSERT INTO silver.order_lines (
    order_line_id,
    transaction_id,
    product_id,
    quantity,
    unit_price,
    discount,
    line_total
)
SELECT
    TRIM(order_line_id)                      AS order_line_id,
    TRIM(transaction_id)                    AS transaction_id,
    TRIM(product_id)                        AS product_id,
    TRY_TO_NUMBER(TRIM(quantity))            AS quantity,
    TRY_TO_NUMBER(TRIM(unit_price))          AS unit_price,
    TRY_TO_NUMBER(TRIM(discount))            AS discount,
    TRY_TO_NUMBER(TRIM(line_total))          AS line_total
FROM sales_db.bronze.bronze_order_lines
WHERE
    -- primary key validity
    order_line_id IS NOT NULL
    AND TRIM(order_line_id) <> ''
    AND order_line_id <> '#N/A'
    AND REGEXP_LIKE(
        UPPER(TRIM(order_line_id)),
        '^OL-[0-9]+$'
    )

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(order_line_id)
    ORDER BY
        -- prefer rows with valid numeric values
        CASE WHEN TRY_TO_NUMBER(quantity)   IS NOT NULL THEN 1 ELSE 2 END,
        CASE WHEN TRY_TO_NUMBER(unit_price) IS NOT NULL THEN 1 ELSE 2 END,
        CASE WHEN TRY_TO_NUMBER(line_total) IS NOT NULL THEN 1 ELSE 2 END
) = 1;

-- create product master table
CREATE OR REPLACE TABLE product (
    product_id    VARCHAR NOT NULL,
    product_name  VARCHAR,
    category      VARCHAR,
    unit_price    NUMBER(10,2),
    load_ts       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- insert to product table--

INSERT INTO silver.product (
    product_id,
    product_name,
    category,
    unit_price
)
SELECT
    TRIM(id)                     AS product_id,
    TRIM(name)                   AS product_name,
    TRIM(category)               AS category,
    TRY_TO_NUMBER(TRIM(unit_price)) AS unit_price
FROM sales_db.bronze.bronze_product_master
WHERE
    -- primary key validity
    id IS NOT NULL
    AND TRIM(id) <> ''
    AND id <> '#N/A'
    AND REGEXP_LIKE(
        UPPER(TRIM(id)),
        '^[A-Z][0-9]+$'
    )

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(id)
    ORDER BY
        -- prefer rows with valid numeric price and non-null attributes
        CASE WHEN TRY_TO_NUMBER(unit_price) IS NOT NULL THEN 1 ELSE 2 END,
        CASE WHEN TRIM(name)     IS NOT NULL AND TRIM(name)     <> '' THEN 1 ELSE 2 END,
        CASE WHEN TRIM(category) IS NOT NULL AND TRIM(category) <> '' THEN 1 ELSE 2 END
) = 1;

-- create sales target table--

CREATE OR REPLACE TABLE sales_targets (
    target_id      VARCHAR NOT NULL,
    employee_id    VARCHAR,
    target_year    NUMBER(4,0),
    target_month   NUMBER(2,0),
    target_amount  NUMBER(12,2),
    load_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- insert to sales target table --

INSERT INTO silver.sales_targets (
    target_id,
    employee_id,
    target_year,
    target_month,
    target_amount
)
SELECT
    TRIM(target_id)                        AS target_id,
    TRIM(employee_id)                      AS employee_id,
    TRY_TO_NUMBER(TRIM(year))              AS target_year,
    TRY_TO_NUMBER(TRIM(month))             AS target_month,
    TRY_TO_NUMBER(TRIM(target_amount))     AS target_amount
FROM sales_db.bronze.bronze_sales_targets
WHERE
    -- primary key validity
    target_id IS NOT NULL
    AND TRIM(target_id) <> ''
    AND target_id <> '#N/A'
    AND REGEXP_LIKE(
        UPPER(TRIM(target_id)),
        '^TGT-[0-9]+$'
    )

    -- basic year/month sanity (avoid bogus values)
    AND TRY_TO_NUMBER(year) BETWEEN 2000 AND 2100
    AND TRY_TO_NUMBER(month) BETWEEN 1 AND 12

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(target_id)
    ORDER BY
        -- prefer rows with valid numeric values and employee_id
        CASE WHEN TRY_TO_NUMBER(target_amount) IS NOT NULL THEN 1 ELSE 2 END,
        CASE WHEN TRIM(employee_id) IS NOT NULL AND TRIM(employee_id) <> '' THEN 1 ELSE 2 END
) = 1;

-- create sales transaction table-- 

CREATE OR REPLACE TABLE sales_transactions (
    transaction_id   VARCHAR NOT NULL,
    transaction_date DATE,
    customer_id      VARCHAR,
    store_id         VARCHAR,
    employee_id      VARCHAR,
    promotion_id     VARCHAR,
    payment_method   VARCHAR,
    total_amount     NUMBER(12,2),
    load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- insert to sales transactions --

INSERT INTO silver.sales_transactions (
    transaction_id,
    transaction_date,
    customer_id,
    store_id,
    employee_id,
    promotion_id,
    payment_method,
    total_amount
)
SELECT
    TRIM(transaction_id)                               AS transaction_id,
    TRY_TO_DATE(TRIM(transaction_date))                AS transaction_date,
    TRIM(customer_id)                                  AS customer_id,
    TRIM(store_id)                                     AS store_id,
    TRIM(employee_id)                                  AS employee_id,
    NULLIF(TRIM(promotion_id), '')                     AS promotion_id,
    TRIM(payment_method)                               AS payment_method,
    TRY_TO_NUMBER(TRIM(total_amount))                  AS total_amount
FROM sales_db.bronze.bronze_sales_transactions
WHERE
    transaction_id IS NOT NULL
    AND TRIM(transaction_id) <> ''
    AND transaction_id <> '#N/A'
    AND REGEXP_LIKE(
        UPPER(TRIM(transaction_id)),
        '^TRN-[0-9]+$'
    )
    AND TRY_TO_DATE(transaction_date) IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(transaction_id)
    ORDER BY
        CASE WHEN TRY_TO_NUMBER(total_amount) IS NOT NULL THEN 1 ELSE 2 END,
        TRY_TO_DATE(transaction_date) DESC
) = 1;


-- create store table --

CREATE OR REPLACE TABLE store (
    store_id     VARCHAR NOT NULL,
    store_name   VARCHAR,
    city         VARCHAR,
    region       VARCHAR,
    load_ts      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    
);


--insert to store table -- 
INSERT INTO silver.store (
    store_id,
    store_name,
    city,
    region
)
SELECT
    TRIM(id)      AS store_id,
    TRIM(name)    AS store_name,
    TRIM(city)    AS city,
    TRIM(region)  AS region
FROM sales_db.bronze.bronze_store_master
WHERE id IS NOT NULL
  AND TRIM(id) <> ''

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(id)
    ORDER BY id
) = 1;
