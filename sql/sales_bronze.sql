USE WAREHOUSE sales_etl_wh;
USE DATABASE sales_db;
USE SCHEMA bronze;

CREATE OR REPLACE TABLE BRONZE.PNRao_Travel_Customers
(
    CUSTOMER_ID      STRING,
    FIRST_NAME       STRING,
    LAST_NAME        STRING,
    EMAIL            STRING,
    PHONE            STRING,
    COUNTRY          STRING
);


COPY INTO bronze_customer_master
FROM @sales_db.raw.sales_stage/PNRao_SalesRetail_Customer_Master.csv
FILE_FORMAT = (FORMAT_NAME = sales_db.raw.csv_file_format)
ON_ERROR = 'CONTINUE';
``
CREATE OR REPLACE TABLE bronze_employee_master (
    employee_id VARCHAR,
    full_name   VARCHAR,
    store_id    VARCHAR
);

COPY INTO bronze_employee_master
FROM @sales_db.raw.sales_stage/PNRao_SalesRetail_Employee_Master.csv
FILE_FORMAT = (FORMAT_NAME = sales_db.raw.csv_file_format)
ON_ERROR = 'CONTINUE';



CREATE OR REPLACE TABLE bronze_inventory_levels (
    inventory_id       VARCHAR,
    product_id         VARCHAR,
    store_id           VARCHAR,
    quantity_on_hand   VARCHAR
);


COPY INTO bronze_inventory_levels
FROM @sales_db.raw.sales_stage/PNRao_SalesRetail_Inventory_Levels.csv
FILE_FORMAT = (FORMAT_NAME = sales_db.raw.csv_file_format)
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE bronze_order_lines (
    order_line_id   VARCHAR,
    transaction_id  VARCHAR,
    product_id      VARCHAR,
    quantity         VARCHAR,
    unit_price       VARCHAR,
    discount         VARCHAR,
    line_total       VARCHAR
);

COPY INTO bronze_order_lines
FROM @sales_db.raw.sales_stage/PNRao_SalesRetail_Order_Lines.csv
FILE_FORMAT = (FORMAT_NAME = sales_db.raw.csv_file_format)
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE bronze_product_master (
    id          VARCHAR,
    name        VARCHAR,
    category    VARCHAR,
    unit_price  VARCHAR
);
``
COPY INTO bronze_product_master
FROM @sales_db.raw.sales_stage/PNRao_SalesRetail_Product_Master.csv
FILE_FORMAT = (FORMAT_NAME = sales_db.raw.csv_file_format)
ON_ERROR = 'CONTINUE';


CREATE OR REPLACE TABLE bronze_sales_targets (
    target_id       VARCHAR,
    employee_id     VARCHAR,
    year             VARCHAR,
    month            VARCHAR,
    target_amount    VARCHAR
);

COPY INTO bronze_sales_targets
FROM @sales_db.raw.sales_stage/PNRao_SalesRetail_Sales_Targets.csv
FILE_FORMAT = (FORMAT_NAME = sales_db.raw.csv_file_format)
ON_ERROR = 'CONTINUE';
``
CREATE OR REPLACE TABLE bronze_sales_transactions (
    transaction_id   VARCHAR,
    transaction_date VARCHAR,
    customer_id      VARCHAR,
    store_id         VARCHAR,
    employee_id      VARCHAR,
    promotion_id     VARCHAR,
    payment_method   VARCHAR,
    total_amount     VARCHAR
);


COPY INTO bronze_sales_transactions
FROM @sales_db.raw.sales_stage/PNRao_SalesRetail_Sales_Transactions.csv
FILE_FORMAT = (FORMAT_NAME = sales_db.raw.csv_file_format)
ON_ERROR = 'CONTINUE';


CREATE OR REPLACE TABLE bronze_store_master (
    id      VARCHAR,
    name    VARCHAR,
    city    VARCHAR,
    region  VARCHAR
);

COPY INTO bronze_store_master
FROM @sales_db.raw.sales_stage/PNRao_SalesRetail_Store_Master.csv
FILE_FORMAT = (FORMAT_NAME = sales_db.raw.csv_file_format)
ON_ERROR = 'CONTINUE';
``

