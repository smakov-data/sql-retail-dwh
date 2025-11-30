/******************************************************************************
    Script:     gold_views_ddl.sql
    Purpose:    Create star-schema views in the Gold layer.

    Description:
    - Defines dimension and fact views on top of the Silver layer.
    - Applies final business-ready transformations and joins.
    - Exposes a clean star schema (dim_* and fact_*) for reporting and analytics.

    Layer:      GOLD (Business-Ready / Reporting Layer)
    Schema:     gold
    Usage:      Query these views directly from BI / analytics tools.

******************************************************************************/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers 
AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY a.cst_key) AS customer_key,
    a.cst_id AS customer_id,
    a.cst_key AS customer_number,
    a.cst_firstname AS first_name,
    a.cst_lastname AS last_name,
    c.cntry AS country,
    a.cst_marital_status AS marital_status,
    CASE 
        WHEN a.cst_gndr != 'Unknown' THEN a.cst_gndr 
        ELSE COALESCE(b.gen, 'Unknown')
    END AS gender,

    b.bdate AS birthdate,
    a.cst_create_date AS create_date
FROM silver.crm_cust_info AS a
LEFT JOIN silver.erp_cust_az12 AS b ON a.cst_key = b.cid
LEFT JOIN silver.erp_loc_a101 AS c ON a.cst_key = c.cid;
GO


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER(ORDER BY p.prd_start_dt, p.prd_key) AS product_key,
    p.prd_id AS product_id,
    p.prd_key AS product_number,
    p.prd_nm AS product_name,
    p.cat_id AS category_id,
    c.cat AS category,
    c.subcat AS subcategory,
    c.maintenance,
    p.prd_cost AS cost,
    p.prd_line AS product_line,
    p.prd_start_dt AS start_date
FROM silver.crm_prd_info AS p
LEFT JOIN silver.erp_px_cat_g1v2 AS c ON p.cat_id = c.id
WHERE p.prd_end_dt IS NULL;
GO


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS 
SELECT 
    s.sls_ord_num AS order_number,
    p.product_key,
    c.customer_key,
    s.sls_order_dt AS order_date,
    s.sls_ship_dt AS shipping_date,
    s.sls_due_dt AS due_date,
    s.sls_sales AS sales_amount,
    s.sls_quantity AS quantity,
    s.sls_price AS price
FROM silver.crm_sales_details AS s
LEFT JOIN gold.dim_products AS p ON s.sls_prd_key = p.product_number
LEFT JOIN gold.dim_customers AS c ON s.sls_cust_id = c.customer_id;
GO
--
