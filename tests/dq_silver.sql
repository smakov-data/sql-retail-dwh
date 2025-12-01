/*
===============================================================================
Quality Checks — Silver Layer
===============================================================================
Purpose:
    Validate the standardized data in the Silver layer before it is consumed 
    by the Gold (analytical) layer. Checks focus on:
        - Primary key integrity (nulls, duplicates)
        - Standardization consistency (trimming, domain values)
        - Numerical and date validity
        - Logical relationships and business rule alignment
        - Correct isolation of invalid records into error tables
          (e.g., crm_prd_cat_errors)

Scope:
    Layer:          SILVER (Cleaned / Standardized Data)
    Validates:      CRM & ERP entities across all Silver tables
    Expected:       Zero records returned for all exception queries

Usage:
    - Run after the Silver ETL pipeline completes.
    - Investigate and remediate any records returned by these checks 
      before proceeding to Gold layer modeling.
===============================================================================
*/


/*==============================================================================
    CRM — Customer Information
==============================================================================*/
-------------------------------
--Primary Key: Uniqueness & Nulls
-------------------------------
SELECT 
    cst_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-------------------------------
--Leading/Trailing Spaces
-------------------------------
SELECT 
    cst_key
FROM silver.crm_cust_info
WHERE cst_key <> TRIM(cst_key);

-------------------------------
--Domain Values: Marital Status
-------------------------------
SELECT DISTINCT 
    cst_marital_status
FROM silver.crm_cust_info;


/*==============================================================================
    CRM — Product Information
==============================================================================*/
-------------------------------
--Primary Key: Uniqueness & Nulls
-------------------------------
SELECT 
    prd_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-------------------------------
--Leading/Trailing Spaces
-------------------------------
SELECT 
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-------------------------------
--Numerical Validation: Cost
-------------------------------
SELECT 
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-------------------------------
--Domain Values: Product Line
-------------------------------
SELECT DISTINCT 
    prd_line
FROM silver.crm_prd_info;

-------------------------------
--Date Validation: Start <= End
-------------------------------
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


/*==============================================================================
    CRM — Product Category Mapping (Error Isolation)
==============================================================================*/
-------------------------------
--Silver products: all categories must exist in ERP category table
--Expectation: zero rows (no invalid category mapping in main table)
-------------------------------
SELECT 
    p.*
FROM silver.crm_prd_info AS p
LEFT JOIN silver.erp_px_cat_g1v2 AS c
    ON p.cat_id = c.id        
WHERE c.id IS NULL;

-------------------------------
--Error table: contains only invalid mappings
--Expectation: zero rows (no valid categories stored as errors)
-------------------------------
SELECT 
    e.*
FROM silver.crm_prd_cat_errors AS e
LEFT JOIN silver.erp_px_cat_g1v2 AS c
    ON e.cat_id = c.id         
WHERE c.id IS NOT NULL;


/*==============================================================================
    CRM — Sales Details
==============================================================================*/
-------------------------------
--Invalid Raw Dates (Bronze Layer)
-------------------------------
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
   OR LEN(sls_due_dt) <> 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;

-------------------------------
--Logical Date Order Checks
-------------------------------
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-------------------------------
--Business Logic: Sales = Quantity * Price
-------------------------------
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


/*==============================================================================
    ERP — Customer Master (az12)
==============================================================================*/
-------------------------------
--Birthdate Validity Range
-------------------------------
SELECT DISTINCT 
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > GETDATE();

-------------------------------
--Domain Values: Gender
-------------------------------
SELECT DISTINCT 
    gen
FROM silver.erp_cust_az12;


/*==============================================================================
    ERP — Location Master (a101)
==============================================================================*/
-------------------------------
--Domain Values: Country Codes
-------------------------------
SELECT DISTINCT
    cntry
FROM silver.erp_loc_a101
ORDER BY cntry;


/*==============================================================================
    ERP — Product Category (g1v2)
==============================================================================*/
-------------------------------
--Leading/Trailing Spaces
-------------------------------
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat        <> TRIM(cat)
   OR subcat     <> TRIM(subcat)
   OR maintenance <> TRIM(maintenance);

-------------------------------
--Domain Values: Maintenance Flags
-------------------------------
SELECT DISTINCT 
    maintenance
FROM silver.erp_px_cat_g1v2;
