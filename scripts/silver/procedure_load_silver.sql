/*
Procedure Script: Silver Layer

Purpose:
This script creates procedure to load data into the 'silver' schema/layer from bronze layer.
It performs the following 2 actions :-
1) Truncates all the tables before loading the data.
2) Perform data preprocessing including data cleaning and standardisation.

The procedure neither accept any parameters nor return any values.

Command to execute:
  EXECUTE silver.load_silver;
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE
		@start_time DATETIME,
		@end_time DATETIME,
		@batch_start_time DATETIME,
		@batch_end_time DATETIME;

BEGIN TRY
	SET @batch_end_time = GETDATE();
	PRINT '========== Loading Silver Layer ==========';

	PRINT '---------- Loading CRM tables ----------';

	PRINT 'Loading Table: silver.crm_cust_info';
	SET @start_time = GETDATE();
	TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info (
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
		TRIM(cst_firstname) AS cst_firstname, -- remvoing unwanted spaces
		TRIM(cst_lastname) AS cst_lastname, -- removing unwanted spaces
		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'N/A' 
		END AS cst_marital_status, -- standardising marital status column
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
			ELSE 'N/A'
		END AS cst_gndr, -- standardising gender column
		cst_create_date
	FROM (
		SELECT
		*
		, RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as rnk
		FROM bronze.crm_cust_info
	) temp 
	WHERE rnk = 1 and cst_id IS NOT NULL; -- filtering null values and fetching latest row for duplicate groups
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @starttime, @endtime) AS NVARCHAR) + ' seconds';

	PRINT 'Loading Table: silver.crm_prd_info';
	SET @start_time = GETDATE();
	TRUNCATE TABLE silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- Extracting category ID
		SUBSTRING(prd_key,7,(LEN(prd_key)-5)) AS prd_key, -- Extracting product key
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost, -- Replacing null values
		CASE TRIM(UPPER(prd_line)) 
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			WHEN 'S' THEN 'Other'
			ELSE 'N/A' -- Standardising product line column
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt, -- changing column type to date
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt -- calculating end date as 1 day before next start date
	FROM bronze.crm_prd_info;
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


	PRINT 'Loading Table: silver.crm_sales_details';
	SET @start_time = GETDATE();
	TRUNCATE TABLE silver.crm_sales_details;
	INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt ,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN LEN(sls_order_dt) != 8 OR sls_order_dt = 0 THEN NULL  -- checking for invalid date
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
		END AS sls_order_dt,
		CASE 
			WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt = 0 THEN NULL  -- checking for invalid date
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
		END AS sls_ship_dt,
		CASE 
			WHEN LEN(sls_due_dt) != 8 OR sls_due_dt = 0 THEN NULL  -- checking for invalid date
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales = sls_quantity * ABS(sls_price)  -- deriving sales using quantity and price if original is invalid
				THEN sls_quantity * ABS(sls_price) 
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0   -- deriving sale price using quantity and sales if original is invalid
				THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details;
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


	PRINT 'Loading Table: silver.erp_cust_az12';
	SET @start_time = GETDATE();
	TRUNCATE TABLE silver.erp_cust_az12;
	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))   -- Removing 'NAS' prefix if present
			ELSE cid 
		END AS cid,
		CASE
			WHEN bdate > GETDATE() THEN NULL   -- setting invalid/future dates to NULL
			ELSE bdate
		END AS bdate,
		CASE
			WHEN UPPER(TRIM(gen)) in ('M','MALE') THEN 'Male'
			WHEN UPPER(TRIM(gen)) in ('F','FEMALE') THEN 'Female'
			ELSE 'N/A'   -- standardising gender column
		END AS gen
	FROM bronze.erp_cust_az12;
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


	PRINT 'Loading Table: silver.erp_loc_a101';
	SET @start_time = GETDATE();
	TRUNCATE TABLE silver.erp_loc_a101;
	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
	)
	SELECT 
		REPLACE(cid,'-','') AS cid,   -- Removing '-' for data consistency
		CASE 
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
			ELSE TRIM(cntry)
		END AS cntry   -- Standardising country column and removing unecessary spaces
	FROM bronze.erp_loc_a101;
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


	PRINT 'Loading Table: silver.erp_px_cat_g1v2';
	SET @start_time = GETDATE();
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	SET @batch_end_time = GETDATE();
	PRINT 'Load Duration for silver layer: ' + CAST(DATEDIFF(second,@batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

END TRY
BEGIN CATCH
	PRINT '========== Error Details ==========';
		PRINT 'Error occurred during loading silver layer';
		PRINT 'Error message: ' + ERROR_MESSAGE();
		PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error state: ' + CAST(ERROR_STATE() AS NVARCHAR);

END CATCH
END;
