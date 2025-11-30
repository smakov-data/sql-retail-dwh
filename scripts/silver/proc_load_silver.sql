/******************************************************************************
    Procedure:  [silver].[load_silver]
    Purpose:    Populate Silver layer tables from Bronze raw data.

    Description:
    - Truncates Silver tables to allow idempotent reloads.
    - Extracts data from Bronze, applies cleansing and business rules.
    - Populates DIM, FACT and error/log tables in the Silver schema.
    - Logs per-table and batch load duration via PRINT statements.

    Layer:      SILVER (Cleaned / Standardized / Conformed Data)
    Schema:     silver
    Parameters: None (no input or output parameters).
    Trigger:    On-demand or scheduled by an ETL/orchestration tool.
    Environment: Dev / Local (non-prod)

******************************************************************************/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME, 
        @batch_start_time DATETIME, 
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';
        PRINT '';
        PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
        
        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12
        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN 
            SUBSTRING(cid, 4,LEN(cid))
            ELSE cid
        END AS cid,
        CASE WHEN bdate > GETDATE() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) LIKE ('M%') THEN 'Male'
            WHEN UPPER(TRIM(gen)) LIKE ('MALE%') THEN 'Male'

            WHEN UPPER(TRIM(gen)) LIKE ('F%') THEN 'Female'
            WHEN UPPER(TRIM(gen)) LIKE ('FEMALE%') THEN 'Female'
            ELSE 'Unknown'
        END AS gen
        FROM bronze.erp_cust_az12

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '  + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds. ----------';
        PRINT ' '

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101
        PRINT '>> Inserting Data Into: silver.erp_loc_a101';

        INSERT INTO silver.erp_loc_a101(
            cid,
            cntry
        )
        SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE 
            WHEN TRIM(clean_cntry_from_select) = 'DE' THEN 'Germany'
            WHEN TRIM(clean_cntry_from_select) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(clean_cntry_from_select) = '' OR clean_cntry_from_select IS NULL THEN 'Unknown'
            ELSE TRIM(clean_cntry_from_select)
        END AS cntry
        FROM
            (
            SELECT 
            TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) AS clean_cntry_from_select,
            cid
            FROM bronze.erp_loc_a101
            )t


        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '  + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds. ----------';
        PRINT ' '

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2
        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

        INSERT INTO silver.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            TRIM(REPLACE(REPLACE(maintenance, CHAR(10), ''), CHAR(13), '')) AS maintenance
        FROM bronze.erp_px_cat_g1v2

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '  + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds. ----------';
        PRINT ' '

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info
        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        
        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'Unknown'
        END AS cst_marital_status,
        
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'Unknown'
        END AS cst_gndr,
        cst_create_date
        FROM 
        (
        SELECT 
            *,
            ROW_NUMBER() OVER
            (PARTITION BY cst_id
            ORDER BY cst_create_date DESC)
            AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
        ) t WHERE flag_last =1 

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '  + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds. ----------';
        PRINT ' '

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info
        PRINT '>> Inserting Data Into: silver.crm_prd_info';

        INSERT INTO silver.crm_prd_info(
            prd_id,
            prd_key,
            cat_id,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            p.prd_id,
            SUBSTRING(prd_key, 7,LEN(prd_key)) AS prd_key,
            REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id,
            p.prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE WHEN UPPER(TRIM(p.prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(p.prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(p.prd_line)) = 'S' THEN 'Other Sales'
                ELSE 'Unknown'
            END prd_line,
            CAST(p.prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(p.prd_start_dt) OVER (PARTITION BY p.prd_key ORDER BY p.prd_start_dt ASC)-1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info AS p

        LEFT JOIN silver.erp_px_cat_g1v2 AS c
            ON REPLACE(SUBSTRING(p.prd_key, 1,5), '-', '_') = c.id
        WHERE c.id IS NOT NULL; --Только те строки, которые при матче по ID не дают нулл - то есть не те что не бьются.

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '  + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds. ----------';
        PRINT ' '

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details
        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
        )
        SELECT
        [sls_ord_num],
        [sls_prd_key],
        [sls_cust_id],

        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,

        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,

        CASE WHEN 
            sls_sales != ABS(sls_price) * sls_quantity OR
            sls_sales IS NULL OR
            sls_sales <=0
        THEN ABS(sls_price) * sls_quantity
        ELSE sls_sales
        END AS sls_sales,

        [sls_quantity],

        CASE WHEN 
            sls_price <=0 OR
            sls_price IS NULL
        THEN sls_sales / NULLIF(sls_quantity, 0) -- нельзя делить на 0, заменяем на NULL
        ELSE sls_price
        END AS sls_price
        FROM bronze.crm_sales_details

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '  + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds. ----------';
        PRINT ' '

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_prd_cat_errors';
        TRUNCATE TABLE silver.crm_prd_cat_errors
        PRINT '>> Inserting Data Into: silver.crm_prd_cat_errors';
        INSERT INTO silver.crm_prd_cat_errors (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,
        --
        error_reason
        )
        SELECT    
        ---
        p.prd_id,
        REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7,LEN(prd_key)) AS prd_key,
        p.prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost,
        CASE WHEN UPPER(TRIM(p.prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(p.prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(p.prd_line)) = 'S' THEN 'Other Sales'
            ELSE 'Unknown'
        END prd_line,
        CAST(p.prd_start_dt AS DATE) AS prd_start_dt,
        CAST(LEAD(p.prd_start_dt) OVER (PARTITION BY p.prd_key ORDER BY p.prd_start_dt ASC)-1 AS DATE) AS prd_end_dt,
        --
        'CATEGORY DOES NOT EXIST' AS error_reason
        FROM bronze.crm_prd_info AS p
        LEFT JOIN silver.erp_px_cat_g1v2 AS c
            ON REPLACE(SUBSTRING(p.prd_key, 1,5), '-', '_') = c.id
        WHERE c.id IS NULL;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '  + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds. ----------';
        PRINT ' '
        
        SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' sec.';
        PRINT '=========================================='
        PRINT '';

    END TRY
    BEGIN CATCH
        PRINT '---------------'
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'Error Message:' + ERROR_MESSAGE();
        PRINT 'Error Number:' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State:' + CAST(ERROR_STATE() AS NVARCHAR); 
    END CATCH
END;