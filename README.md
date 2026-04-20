# Sales & Retail Analytics Platform (Snowflake)

This project implements a complete **end-to-end analytics data platform**
using **Snowflake SQL**, following **Medallion Architecture (Bronze, Silver, Gold)**
and **Star Schema modeling** best practices.

The solution is designed to support **business intelligence and analytics use cases**
such as sales performance tracking, target vs actual analysis, and inventory monitoring,
with final consumption through **Power BI**.

---

## Architecture Overview

The platform follows the Medallion Architecture pattern:

### 🥉 Bronze Layer
- Raw ingestion of CSV files
- No transformations applied
- Preserves source-system data as-is

### 🥈 Silver Layer
- Data cleansing and standardization
- Primary key validation and deduplication
- Window-function-based transformations
- Data type enforcement

### 🥇 Gold Layer
- Analytical star schema
- Dimensions and fact tables
- Optimized for BI and reporting tools
- Designed for Power BI consumption

---

## Gold Data Model

### Dimensions
- `dim_date`
- `dim_customer`
- `dim_product`
- `dim_store`
- `dim_employee`

### Fact Tables
- `fact_sales` (order-line grain)
- `fact_sales_targets` (employee-month grain)
- `fact_inventory_snapshot` (product-store snapshot)

---

## Data Quality & Validation

Data quality checks are explicitly implemented in the Silver layer and include:
- Null and empty primary key checks
- Invalid format detection
- Duplicate record identification
- Health summary queries

These checks ensure that only trusted, analytics-ready data reaches the Gold layer.


