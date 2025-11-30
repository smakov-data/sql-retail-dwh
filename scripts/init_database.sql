/******************************************************************************
    Script:     init_database_and_schemas.sql
    Purpose:    Create (or recreate) the DataWarehouse database and core schemas.

    Description:
    - Drops the existing 'DataWarehouse' database if it exists.
    - Recreates the database from scratch.
    - Creates the three core schemas used in the medallion architecture:
        • bronze  – raw/landing zone
        • silver  – cleaned/standardized data
        • gold    – business-ready analytics layer

    WARNING:
    - This script DROPS the entire DataWarehouse database.
    - All objects and data will be permanently deleted.
    - Run only in development/local environments.

    Environment: Dev / Local (non-prod)
******************************************************************************/

USE master;
GO

-------------------------------------------------------------
--Drop and recreate database
-------------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-------------------------------------------------------------
--Create Schemas (Bronze, Silver, Gold)
-------------------------------------------------------------

--Bronze schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze AUTHORIZATION dbo');
GO

--Silver schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver AUTHORIZATION dbo');
GO

--Gold schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold AUTHORIZATION dbo');
GO

PRINT 'Database and schemas successfully initialized.';
GO
