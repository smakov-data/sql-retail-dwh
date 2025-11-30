## Enterprise Sales Data Warehouse (Medallion Architecture)

This repository contains a full SQL-based Data Warehouse implemented on SQL Server.
The project demonstrates modern data engineering practices used in enterprise environments: Medallion Architecture, ETL pipelines, data quality controls, dimensional modeling, and analytical data marts.

## 1. Architecture Overview

The solution follows the Medallion model (Bronze → Silver → Gold):

Bronze Layer
Stores raw CRM and ERP datasets ingested from CSV files.
No transformations are applied; tables represent source systems as-is.

Silver Layer
Cleansed and standardized data.
Includes data validation, trimming, null handling, normalization, derived columns, and enrichment from multiple sources.
Invalid or inconsistent records are isolated into error tables.

Gold Layer
Business-ready data modeled as a star schema:
- fact_sales
- dim_customers
- dim_products

Designed for reporting, analytics, and ad-hoc SQL queries.
