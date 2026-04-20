USE WAREHOUSE sales_etl_wh;
USE DATABASE sales_db;
USE SCHEMA gold;

-- create table date --

CREATE OR REPLACE TABLE dim_date (
    date_key        NUMBER(8,0) PRIMARY KEY,
    calendar_date   DATE NOT NULL,

    day             NUMBER(2,0),
    day_name        VARCHAR,
    day_of_week     NUMBER(1,0),
    is_weekend      BOOLEAN,

    month            NUMBER(2,0),
    month_name       VARCHAR,
    month_short      VARCHAR,
    year_month       VARCHAR,

    quarter          NUMBER(1,0),
    year             NUMBER(4,0)
);

-- populate date table --

INSERT INTO dim_date
SELECT
    TO_NUMBER(TO_VARCHAR(d, 'YYYYMMDD'))               AS date_key,
    d                                                   AS calendar_date,

    DAY(d)                                              AS day,
    DAYNAME(d)                                          AS day_name,
    DAYOFWEEKISO(d)                                     AS day_of_week,
    CASE WHEN DAYOFWEEKISO(d) IN (6,7) THEN TRUE ELSE FALSE END
                                                        AS is_weekend,

    MONTH(d)                                            AS month,
    MONTHNAME(d)                                        AS month_name,
    LEFT(MONTHNAME(d), 3)                               AS month_short,
    TO_VARCHAR(d, 'YYYY-MM')                            AS year_month,

    QUARTER(d)                                          AS quarter,
    YEAR(d)                                             AS year
FROM (
    SELECT DATEADD(
        DAY,
        SEQ4(),
        DATE '2018-01-01'
    ) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 5000))
)
WHERE d <= DATE '2030-12-31';

-- create customer table --
CREATE OR REPLACE TABLE dim_customer (
    customer_key    NUMBER(38,0) AUTOINCREMENT,
    customer_id     VARCHAR NOT NULL,
    full_name       VARCHAR,
    load_ts         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
);

--insert to table costomers --
INSERT INTO gold.dim_customer (
    customer_id,
    full_name
)
SELECT
    customer_id,
    full_name
FROM silver.customer;

--create product table --
CREATE OR REPLACE TABLE dim_product (
    product_key     NUMBER(38,0) AUTOINCREMENT,
    product_id      VARCHAR NOT NULL,
    product_name    VARCHAR,
    category         VARCHAR,
    unit_price       NUMBER(10,2),
    load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_dim_product PRIMARY KEY (product_key)
);

--insert to procust table--
INSERT INTO gold.dim_product (
    product_id,
    product_name,
    category,
    unit_price
)
SELECT
    product_id,
    product_name,
    category,
    unit_price
FROM silver.product;

-- create table store --
CREATE OR REPLACE TABLE dim_store (
    store_key    NUMBER(38,0) AUTOINCREMENT,
    store_id     VARCHAR NOT NULL,
    store_name   VARCHAR,
    city         VARCHAR,
    region       VARCHAR,
    load_ts      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_dim_store PRIMARY KEY (store_key)
);

-- insert to table store --

INSERT INTO gold.dim_store (
    store_id,
    store_name,
    city,
    region
)
SELECT
    store_id,
    store_name,
    city,
    region
FROM silver.store;

-- create table employee --

CREATE OR REPLACE TABLE dim_employee (
    employee_key   NUMBER(38,0) AUTOINCREMENT,
    employee_id    VARCHAR NOT NULL,
    full_name      VARCHAR,
    store_id       VARCHAR,
    load_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_dim_employee PRIMARY KEY (employee_key)
);

-- insert to table employee --

INSERT INTO gold.dim_employee (
    employee_id,
    full_name,
    store_id
)
SELECT
    employee_id,
    full_name,
    store_id
FROM silver.employee;

-- create table,fact sales --
CREATE OR REPLACE TABLE fact_sales (
    sales_fact_key   NUMBER(38,0) AUTOINCREMENT,

    -- Dimension keys
    date_key         NUMBER(8,0),
    customer_key     NUMBER(38,0),
    product_key      NUMBER(38,0),
    store_key        NUMBER(38,0),
    employee_key     NUMBER(38,0),

    -- Degenerate dimensions
    transaction_id   VARCHAR,
    order_line_id    VARCHAR,

    -- Measures
    quantity         NUMBER(10,0),
    unit_price       NUMBER(10,2),
    discount         NUMBER(10,2),
    line_total       NUMBER(12,2),

    load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_fact_sales PRIMARY KEY (sales_fact_key)
);

-- insert to table sales --

INSERT INTO gold.fact_sales (
    date_key,
    customer_key,
    product_key,
    store_key,
    employee_key,
    transaction_id,
    order_line_id,
    quantity,
    unit_price,
    discount,
    line_total
)
SELECT
    dd.date_key,
    dc.customer_key,
    dp.product_key,
    ds.store_key,
    de.employee_key,

    st.transaction_id,
    ol.order_line_id,

    ol.quantity,
    ol.unit_price,
    ol.discount,
    ol.line_total
FROM silver.order_lines ol

JOIN silver.sales_transactions st
  ON ol.transaction_id = st.transaction_id

-- Date dimension
JOIN gold.dim_date dd
  ON st.transaction_date = dd.calendar_date

-- Customer
LEFT JOIN gold.dim_customer dc
  ON st.customer_id = dc.customer_id

-- Product
LEFT JOIN gold.dim_product dp
  ON ol.product_id = dp.product_id

-- Store
LEFT JOIN gold.dim_store ds
  ON st.store_id = ds.store_id

-- Employee
LEFT JOIN gold.dim_employee de
  ON st.employee_id = de.employee_id;

  -- create table sales target --
  CREATE OR REPLACE TABLE fact_sales_targets (
    sales_target_fact_key NUMBER(38,0) AUTOINCREMENT,

    -- Dimension keys
    employee_key          NUMBER(38,0),
    date_key              NUMBER(8,0),

    -- Degenerate dimension
    target_id             VARCHAR,

    -- Measure
    target_amount         NUMBER(12,2),

    load_ts               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_fact_sales_targets PRIMARY KEY (sales_target_fact_key)
);


-- create salest target table --
CREATE OR REPLACE TABLE fact_sales_targets (
    sales_target_fact_key NUMBER(38,0) AUTOINCREMENT,

    -- Dimension keys
    employee_key          NUMBER(38,0),
    date_key              NUMBER(8,0),

    -- Degenerate dimension
    target_id             VARCHAR,

    -- Measure
    target_amount         NUMBER(12,2),

    load_ts               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_fact_sales_targets PRIMARY KEY (sales_target_fact_key)
);


  -- insert to table sales target --

  INSERT INTO gold.fact_sales_targets (
    employee_key,
    date_key,
    target_id,
    target_amount
)
SELECT
    de.employee_key,
    dd.date_key,
    st.target_id,
    st.target_amount
FROM silver.sales_targets st

-- Join employee dimension
JOIN gold.dim_employee de
  ON st.employee_id = de.employee_id

-- Convert year + month to a date and join to dim_date
JOIN gold.dim_date dd
  ON dd.calendar_date = DATE_FROM_PARTS(
        st.target_year,
        st.target_month,
        1
     );

     CREATE OR REPLACE TABLE fact_inventory_snapshot (
    inventory_fact_key NUMBER(38,0) AUTOINCREMENT,

    -- Dimension keys
    product_key        NUMBER(38,0),
    store_key          NUMBER(38,0),

    -- Degenerate dimension
    inventory_id       VARCHAR,

    -- Measure
    quantity_on_hand   NUMBER(10,0),

    snapshot_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_fact_inventory PRIMARY KEY (inventory_fact_key)
);

INSERT INTO gold.fact_inventory_snapshot (
    product_key,
    store_key,
    inventory_id,
    quantity_on_hand
)
SELECT
    dp.product_key,
    ds.store_key,
    il.inventory_id,
    il.quantity_on_hand
FROM silver.inventory_levels il

-- Product dimension
JOIN gold.dim_product dp
  ON il.product_id = dp.product_id

-- Store dimension
JOIN gold.dim_store ds
  ON il.store_id = ds.store_id;

 SELECT
    dp.product_name,
    ds.store_name,
    fis.quantity_on_hand
FROM gold.fact_inventory_snapshot fis
JOIN gold.dim_product dp
  ON fis.product_key = dp.product_key
JOIN gold.dim_store ds
  ON fis.store_key = ds.store_key
ORDER BY fis.quantity_on_hand DESC;
