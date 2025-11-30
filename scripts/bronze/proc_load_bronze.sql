/******************************************************************************
    Procedure:  [bronze].[load_bronze]
    Purpose:    Load raw CRM and ERP data from CSV files into the Bronze layer.

    Description:
    - Truncates Bronze tables to allow idempotent reloads.
    - Uses BULK INSERT to load CSV files from /var/opt/mssql/import.
    - Logs per-table and batch load duration using PRINT statements.
    - Handles failures via TRY...CATCH and prints basic error details.

    Layer:      BRONZE (Raw / Landing Zone)
    Schema:     bronze
    Trigger:    On-demand or scheduled by ETL/orchestration tool.
    Environment: Dev / Local (non-prod)

******************************************************************************/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';
        PRINT '';
		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
        
        ----------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info;
        
        PRINT '>> Inserting Data Into: bronze.crm_cust_info'
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/import/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        ----------------------------
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Truncating Table: bronze.crm_prd_info'
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info'
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/import/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        ----------------------------
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Truncating Table: bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details'
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/import/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        ----------------------------
        SET @start_time = GETDATE();
        
        PRINT '';
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
        
        PRINT '>> Truncating Table: bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12'
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/import/cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        ----------------------------
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Truncating Table: bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101'
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/import/loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        ----------------------------
        SET @start_time = GETDATE();
        PRINT '';
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/import/px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        ----------------------------
        SET @batch_end_time = GETDATE();
        PRINT '-----------------------------------------------------------------------';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' sec.';
        PRINT '';
        PRINT '';
    END TRY
    ----------------------------
    BEGIN CATCH
        PRINT '---------------'
        PRINT 'ERROR BRONZE'
        PRINT 'Error Message:' + ERROR_MESSAGE();
        PRINT 'Error Number:' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State:' + CAST(ERROR_STATE() AS NVARCHAR); 
    END CATCH
END;    
