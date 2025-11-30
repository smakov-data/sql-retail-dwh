/*
===============================================================================
Quality Checks — Gold Layer
===============================================================================
Purpose:
    Validate the integrity and analytical readiness of the Gold layer. 
    These checks confirm:
        - Uniqueness of surrogate keys in dimension tables.
        - Referential integrity between fact and dimension tables.
        - Correct relationship mapping aligned with the star schema.

Scope:
    Layer:          GOLD (Business-Ready Data)
    Validates:      dim_customers, dim_products, fact_sales
    Expected Result: All queries return zero rows.

Usage:
    - Run after Gold layer build.
    - Investigate and resolve any non-zero results before releasing data
      to BI/Reporting consumers.
===============================================================================
*/

-------------------------------
--Uniqueness: dim_customers
-------------------------------
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-------------------------------
--Uniqueness: dim_products
-------------------------------
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-------------------------------
--Referential Integrity: fact_sales
-------------------------------
SELECT 
    f.*
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products AS p
    ON p.product_key = f.product_key
WHERE c.customer_key IS NULL 
   OR p.product_key IS NULL;
