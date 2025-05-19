/*
Create Database and Schemas
Purpose:
    The script creates a database named 'DataWarehouse' after checking its existence. 
    If the database exists, it will be dropped and recreated. Additionaly, the script creates
    3 schemas within the database: 'bronze', 'silver', 'gold'.
*/

USE master;
GO

-- drop and create new database 'DataWarehouse' 
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

USE DataWarehouse;
GO

-- create the schemas for the layers
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
