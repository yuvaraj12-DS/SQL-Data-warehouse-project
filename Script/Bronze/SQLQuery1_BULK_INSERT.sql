USE master;

CREATE DATABASE DataWarehouse;
USE DataWarehouse;

CREATE SCHEMA bronze;
go
CREATE SCHEMA silver;
go
CREATE SCHEMA gold;
go

use DataWarehouse;
---CREATING STORED PROCEDURES
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
     DECLARE @Start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
     BEGIN TRY
	 SET @batch_start_time =GETDATE();
     PRINT '====================================================';
     PRINT 'Loading Bronze Layer';
	 PRINT '====================================================';

	 PRINT '----------------------------------------------------';
     PRINT 'Loading CRM Tables';
	 PRINT '----------------------------------------------------';

   SET @Start_time =GETDATE();
TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'E:\SQL\DW_ETL_BARAA\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
    SET @end_time=GETDATE();
	PRINT '>> LOAD DURATION: '+ CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR)+'SECONDS';
	print'>>-----------------------';

SELECT*FROM bronze.crm_cust_info;
SELECT COUNT(*) FROM bronze.crm_cust_info;

SET @Start_time =GETDATE();
TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM 'E:\SQL\DW_ETL_BARAA\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
  SET @end_time=GETDATE();
  PRINT '>> LOAD DURATION: '+ CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR)+'SECONDS';
	print'>>-----------------------';

SELECT*FROM bronze.crm_prd_info;
SELECT COUNT(*) FROM bronze.crm_prd_info;

SET @Start_time =GETDATE();
TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'E:\SQL\DW_ETL_BARAA\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
  SET @end_time=GETDATE();
  PRINT '>> LOAD DURATION: '+ CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR)+'SECONDS';
	print'>>-----------------------';

SELECT*FROM bronze.crm_sales_details;
SELECT COUNT(*) FROM bronze.crm_sales_details;


      PRINT '----------------------------------------------------';
      PRINT 'Loading ERP Tables';
	  PRINT '----------------------------------------------------';

	  SET @Start_time =GETDATE();
TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM 'E:\SQL\DW_ETL_BARAA\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
 SET @end_time=GETDATE();
 PRINT '>> LOAD DURATION: '+ CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR)+'SECONDS';
	print'>>-----------------------';

SELECT*FROM bronze.erp_loc_a101;
SELECT COUNT(*) FROM bronze.erp_loc_a101;

SET @Start_time =GETDATE();
TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM 'E:\SQL\DW_ETL_BARAA\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
 SET @end_time=GETDATE();
 PRINT '>> LOAD DURATION: '+ CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR)+'SECONDS';
	print'>>-----------------------';

SELECT*FROM bronze.erp_cust_az12;
SELECT COUNT(*) FROM bronze.erp_cust_az12;

SET @Start_time =GETDATE();
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'E:\SQL\DW_ETL_BARAA\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH(
FIRSTROW=2,
FIELDTERMINATOR=',',
TABLOCK
);
SET @end_time=GETDATE();
PRINT '>> LOAD DURATION: '+ CAST(DATEDIFF(SECOND, @Start_time, @end_time) AS NVARCHAR)+'SECONDS';
	print'>>-----------------------';

	 SET @batch_end_time= GETDATE();
	 PRINT '>> LOAD DURATION of BATCH: '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)+'SECONDS';
	print'>>-----------------------';
SELECT*FROM bronze.erp_px_cat_g1v2;
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
 END TRY
 BEGIN CATCH
       PRINT '====================================================';
       PRINT 'Error in Loading Bronze Layer';
	   PRINT 'Error massage' + ERROR_MESSAGE();
	   PRINT 'Error MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR);
	   PRINT 'Error MESSAGE'+ CAST(ERROR_STATE() AS NVARCHAR);
	 PRINT '====================================================';
 END CATCH
END
