/*
*******************************************
DDL Script : Gold Layer
*******************************************
Purpose:
This script creates 3 views for the gold layer in the datawarehouse.
It combines data from the silver layer and produce enriched business-ready data which can be further used for data analytics and reporting purposes.
*/


-- creating dimension table: customers
CREATE VIEW gold.dim_customers AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	CASE
		WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen,'N/A')
	END AS gender,
	ca.bdate AS birth_date,
	ci.cst_marital_status AS marital_status,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci -- Master Table
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid;


-- creating dimension table: products
CREATE VIEW gold.dim_products AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key) AS product_key,
	pi.prd_id AS product_id,
	pi.prd_key AS product_number,
	pi.prd_nm AS product_name,
	pi.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance AS maintenance_requried,
	pi.prd_cost AS cost,
	pi.prd_line AS product_line,
	pi.prd_start_dt AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc 
	ON pi.cat_id = pc.id
WHERE pi.prd_end_dt is NULL; -- filtering all historical data



-- creating fact table: sales records
CREATE VIEW gold.fact_sales AS 
SELECT 
sd.sls_ord_num AS order_number,
pr.product_key,
c.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
	ON sd.sls_prd_key = pr.product_number 
LEFT JOIN gold.dim_customers c
	ON sd.sls_cust_id = c.customer_id;
