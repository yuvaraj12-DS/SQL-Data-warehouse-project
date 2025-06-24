SELECT TOP (1000) [cst_id]
      ,[cst_key]
      ,[cst_firstname]
      ,[cst_lastname]
      ,[cst_marital_status]
      ,[cst_gndr]
      ,[cst_create_date]
  FROM [DataWarehouse].[bronze].[crm_cust_info]

SELECT TOP (1000) [prd_id]
      ,[prd_key]
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
  FROM [DataWarehouse].[bronze].[crm_prd_info]

SELECT TOP (1000) [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sales_details]

SELECT TOP 1000 * FROM bronze.erp_cust_az12;
SELECT TOP 1000 * FROM bronze.erp_loc_a101;
SELECT TOP 1000 * FROM bronze.erp_px_cat_g1v2;

---silver layer tables creation---
USE DataWarehouse;
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
	cat_id       NVARCHAR(50),
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-----check for the nulls and duplicates---
---expectation: no result

SELECT
*
FROM bronze.crm_cust_info;

SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;

---CONCLUSION: There is duplicates as well as Null id's. which should not exist in case of primary key.

SELECT * FROM bronze.crm_cust_info
WHERE cst_id=29466;

--using window fun rank and select the highest one which will be the newwest one...

SELECT * FROM (SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as FLAG_LAST
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL) t 
WHERE FLAG_LAST =1;

---Check for unwanted spaces---
---EXPE:NO results

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname); 

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname); 
---cleaning spaces
SELECT cst_id, cst_key, TRIM(cst_firstname) AS cst_firstname, TRIM(cst_lastname) AS cst_lastname, cst_gndr, cst_marital_status, cst_create_date
FROM (SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as FLAG_LAST
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL) t 
WHERE FLAG_LAST =1;

---data consistency and standerdization
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

TRUNCATE TABLE silver.crm_cust_info
INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_gndr,
cst_marital_status,
cst_create_date)
SELECT cst_id, 
cst_key, 
TRIM(cst_firstname) AS cst_firstname, 
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
	 ELSE 'N/A'
END cst_gndr,
CASE WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
     WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
	 ELSE 'N/A'
END cst_marital_status, 
cst_create_date
FROM (SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as FLAG_LAST
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL) t 
WHERE FLAG_LAST =1;

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info;

-----------same procedure for product table

SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') NOT IN
(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
     WHEN UPPER(TRIM(prd_line))='R' THEN 'ROAD'
	 WHEN UPPER(TRIM(prd_line))='S' THEN 'Othr Sales'
	 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
	 ELSE 'N/A'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN
(SELECT sls_prd_key FROM bronze.crm_sales_details);

--check for nulls or neg no.
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <0 or prd_cost IS NULL;

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

---cheking invalid date order

SELECT* FROM bronze.crm_prd_info
WHERE prd_end_dt <prd_start_dt;

INSERT INTO silver.crm_prd_info(
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
REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
     WHEN UPPER(TRIM(prd_line))='R' THEN 'ROAD'
	 WHEN UPPER(TRIM(prd_line))='S' THEN 'Othr Sales'
	 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
	 ELSE 'N/A'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

SELECT * FROM silver.crm_prd_info;

INSERT INTO silver.crm_sales_details(
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
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales !=sls_quantity*ABS(sls_price)
      THEN sls_quantity*ABS(sls_price)
	  ELSE sls_sales
END sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <=0
     THEN sls_sales/NULLIF(sls_quantity,0)
	 ELSE sls_price
END sls_price
FROM bronze.crm_sales_details;
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

--check for invalid date
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0 ---replace it with null in case of zero
OR LEN(sls_order_dt) !=8 
OR sls_order_dt<19000101
OR sls_order_dt>20500101

--check for invalid date order
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt> sls_due_dt

---data consistency b/t sales , quantity and price
-->> sales =quantity * price
-->> values must not be NULL, ZERO OR NEGATIVE
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales !=sls_quantity*ABS(sls_price)
      THEN sls_quantity*ABS(sls_price)
	  ELSE sls_sales
END sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <=0
     THEN sls_sales/NULLIF(sls_quantity,0)
	 ELSE sls_price
END sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price<=0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT * FROM silver.crm_sales_details;

----erp tables cleaning
INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'N/A'
END gen
FROM bronze.erp_cust_az12
WHERE cid like '%AW00011000%'

---IDENTIFCATION OF OUT OF RANGE BIRTHDATE
SELECT DISTINCT
bdate 
FROM bronze.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > getdate()

SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'N/A'
END gen
FROM bronze.erp_cust_az12

SELECT*FROM silver.erp_cust_az12;

----LOCATION CLEAN
INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT 
REPLACE(cid, '-','') cid,
CASE WHEN TRIM(cntry)= 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) ='' OR  cntry IS NULL THEN 'N/A'
	 ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-','') NOT IN
(SELECT cst_key FROM silver.crm_cust_info);

SELECT DISTINCT
cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

SELECT* FROM silver.erp_loc_a101;

---CATEGORY CLEANING
INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

----check for unwanted soaces
SELECT 
*
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat!=TRIM(subcat)
OR maintenance!=TRIM(maintenance)

---DATA CONSIS AND STADERDISE
SELECT DISTINCT
maintenance
from bronze.erp_px_cat_g1v2

SELECT *FROM silver.erp_px_cat_g1v2;