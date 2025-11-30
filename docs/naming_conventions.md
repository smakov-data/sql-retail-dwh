# Naming Conventions

This document defines the naming standards for schemas, tables, columns, views, and stored procedures used in the data warehouse.
The goal is to keep all objects consistent, readable, and aligned with enterprise and consulting best practices.

## 1. General Principles

Use snake_case for all database objects.

Use English for all names.

Avoid SQL reserved keywords.

Names must be descriptive and consistent.

Prefer clarity over abbreviation.

## 2. Schemas

Schemas represent Medallion layers:

bronze – raw landing zone

silver – cleaned, standardized, enriched data

gold – semantic business model (views)

## 3. Table Naming
### 3.1 Bronze Layer

Purpose: raw ingestion, structure mirrors source systems.
Pattern: <sourcesystem>_<entity>

Rules:

Must match source table/entity name.

No renaming or business adjustments.

Examples:

bronze.crm_cust_info

bronze.crm_prd_info

bronze.crm_sales_details

bronze.erp_cust_az12

bronze.erp_loc_a101

bronze.erp_px_cat_g1v2

### 3.2 Silver Layer

Purpose: standardized, cleaned, enriched data, traceable to source.
Pattern: <sourcesystem>_<entity>

Rules:

Same base name as Bronze.

Column structure may change due to validation, trimming, normalization, enrichment.

Examples:

silver.crm_cust_info

silver.crm_prd_info

silver.crm_sales_details

silver.erp_cust_az12

silver.erp_loc_a101

silver.erp_px_cat_g1v2

### 3.3 Error Tables (Silver)

Purpose: isolate invalid or inconsistent records detected during Silver processing.
Pattern: <entity>_errors

Examples:

silver.crm_prd_cat_errors

### 3.4 Gold Layer (Views)

Purpose: business-oriented analytical model (star schema).
Pattern: dim_<entity>, fact_<entity>, optionally report_<entity>

Examples:

gold.dim_customers

gold.dim_products

gold.fact_sales

Gold objects are SQL views but use analytical table naming because the gold schema already indicates their semantic role. No vw_ prefix is used.

## 4. Column Naming
### 4.1 Business Columns

Use descriptive names reflecting business meaning.

Examples:

customer_id

customer_number

product_name

category_id

order_date

sales_amount

### 4.2 Surrogate Keys

Pattern: <entity>_key
Examples:

customer_key

product_key

Rules:

Used only in Silver/Gold.

Do not replace business/natural keys.

### 4.3 Technical Columns

Pattern: dwh_<attribute>
Examples:

dwh_load_date

dwh_update_ts

dwh_batch_id

Purpose: ETL metadata, lineage, auditability.

## 5. Stored Procedures

Stored procedures follow layer-based naming.

Pattern: load_<layer>
Examples:

load_bronze

load_silver

load_gold

Additional specialized procedures may extend the pattern.

## 6. Views (Gold Layer)

Gold layer objects are implemented as SQL views but follow business naming conventions used for analytical models. The gold schema itself provides context, so vw_ prefixes are not required.

Examples:

gold.fact_sales

gold.dim_customers

gold.dim_products

## 7. Naming Summary
Object Type	Pattern	Example
Bronze tables	<source>_<entity>	bronze.crm_sales_details
Silver tables	<source>_<entity>	silver.erp_cust_az12
Error tables	<entity>_errors	silver.crm_prd_cat_errors
Gold dimensions	dim_<entity>	gold.dim_customers
Gold facts	fact_<entity>	gold.fact_sales
Surrogate keys	<entity>_key	customer_key
Technical columns	dwh_<attribute>	dwh_load_date
Stored procedures	load_<layer>	load_silver
