/*
===============================================================================
Load Script: Load Bronze Tables
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - And loads data from csv Files to bronze tables.
===============================================================================
*/

SELECT 'Loading CRM Tables...' AS message;
SELECT 'Truncating Table: bronze.crm_cust_info' AS message;
set @start_time = now();
truncate table bronze.crm_cust_info;
SELECT 'Inserting data into Table: bronze.crm_cust_info' AS message;
LOAD DATA LOCAL INFILE 'C:/Users/disha/Downloads/f78e076e5b83435d84c6b6af75d8a679/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT 'Truncating Table: bronze.crm_prod_info' AS message;
truncate table bronze.crm_prod_info;
SELECT 'Inserting data into Table: bronze.crm_prod_info' AS message;
LOAD DATA LOCAL INFILE 'C:/Users/disha/Downloads/f78e076e5b83435d84c6b6af75d8a679/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prod_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT 'Truncating Table: bronze.crm_sales_details' AS message;
truncate table bronze.crm_sales_details;
SELECT 'Inserting data into Table: bronze.crm_sales_details' AS message;
LOAD DATA LOCAL INFILE 'C:/Users/disha/Downloads/f78e076e5b83435d84c6b6af75d8a679/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT 'Truncating Table: bronze.erp_cust_az12' AS message;
truncate table bronze.erp_cust_az12;
SELECT 'Inserting data into Table: bronze.erp_cust_az12' AS message;
LOAD DATA LOCAL INFILE 'C:/Users/disha/Downloads/f78e076e5b83435d84c6b6af75d8a679/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT 'Truncating Table: bronze.erp_loc_a101' AS message;
truncate table bronze.erp_loc_a101;
SELECT 'Inserting data into Table: bronze.erp_loc_a101' AS message;
LOAD DATA LOCAL INFILE 'C:/Users/disha/Downloads/f78e076e5b83435d84c6b6af75d8a679/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT 'Truncating Table: bronze.erp_px_cat_g1v2' AS message;
truncate table bronze.erp_px_cat_g1v2;
SELECT 'Inserting data into Table: bronze.erp_px_cat_g1v2' AS message;
LOAD DATA LOCAL INFILE 'C:/Users/disha/Downloads/f78e076e5b83435d84c6b6af75d8a679/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SELECT 'Loading CRM Tables completed...' AS msg;
set @end_time = now();
SELECT 
    TIMESTAMPDIFF(second,
        @start_time,
        @end_time) as total_time_taken;

