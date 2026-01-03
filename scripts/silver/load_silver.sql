/*
===============================================================================
Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This script performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
  Handle cases like:
    - Check for nulls or duplicates in Primary key.
    - Check unwanted spaces in string datatypes.
    - Data Standardization & Consistency.
===============================================================================
*/

    
-- Inserting data into table silver.crm_cust_info
TRUNCATE TABLE silver.crm_cust_info;
 insert into silver.crm_cust_info( 
 cst_id,
 cst_key,
 cst_firstname,
 cst_lastname,
 cst_marital_status,
 cst_gender,
 cst_create_date
 )
select cst_id, cst_key, trim(cst_firstname) as cst_firstname, trim(cst_lastname) as cst_lastname, 
case when upper(trim(cst_marital_status))='M' then "Married"
     when upper(trim(cst_marital_status))='S' then "Single"
     Else 'n/a'
end as cst_marital_status
,
case when upper(trim(cst_gender))='M' then "Male"
     when upper(trim(cst_gender))='F' then "Female"
     Else 'n/a'
end as cst_gender, cst_create_date from
(
select *, row_number() over (partition by cst_id order by cst_create_date desc) as flag_last 
from 
(select  cst_id,
 cst_key,
 cst_firstname,
 cst_lastname,
 cst_marital_status,
 cst_gender,
 case when cst_create_date=0000-00-00 then null else cst_create_date end as cst_create_date from bronze.crm_cust_info)f
)t
where flag_last = 1;


-- Inserting data into table silver.crm_prod_info
TRUNCATE TABLE silver.crm_prod_info;
insert into silver.crm_prod_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
select 
prd_id,
replace(substring(prd_key,1,5),'-','_') as cat_id,
substring(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
ifnull(prd_cost,0) as prd_cost,
case when upper(trim(prd_line))='M' then 'Mountain'
     when upper(trim(prd_line))='R' then 'Road'
     when upper(trim(prd_line))='S' then 'Other Sales'
     when upper(trim(prd_line))='T' then 'Touring'
     else 'n/a'
end as prd_line,
prd_start_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY AS prd_end_dt
from bronze.crm_prod_info;


-- Inserting data into silver.crm_sales_details
TRUNCATE TABLE silver.crm_sales_details;
insert into silver.crm_sales_details(
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
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt=0 or length(sls_order_dt)!=8 then null
     else STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
end as sls_order_dt,
case when sls_ship_dt=0 or length(sls_ship_dt)!=8 then null
     else STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
end as sls_ship_dt,
case when sls_due_dt=0 or length(sls_due_dt)!=8 then null
     else STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
end as sls_due_dt,
case when sls_sales is null or sls_sales <= 0 then sls_quantity * abs(sls_price)
     when sls_sales != sls_quantity * sls_price and sls_quantity is not null and sls_price is not null and sls_quantity !=0 and sls_price !=0
         then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price <= 0 
         then sls_sales / nullif(sls_quantity,0)
	else sls_price
end as sls_price
 
from bronze.crm_sales_details;


-- Inserting data into silver.erp_cust_az12
TRUNCATE TABLE silver.erp_cust_az12;
insert into silver.erp_cust_az12(cid,bdate,gen)
select
case when cid like "NAS%" then substring(cid,4,length(cid)) 
     else cid
end as cid, 
case when bdate > current_date() then null
else bdate end as bdate, 
CASE
    WHEN UPPER(
        TRIM(
            REPLACE(
                REPLACE(gen, CHAR(13), ''),
            CHAR(10), '')
        )
    ) IN ('F','FEMALE') THEN 'Female'

    WHEN UPPER(
        TRIM(
            REPLACE(
                REPLACE(gen, CHAR(13), ''),
            CHAR(10), '')
        )
    ) IN ('M','MALE') THEN 'Male'

    ELSE 'n/a'
END AS gen
 from bronze.erp_cust_az12;
 
 
 -- Inserting data into silver.erp_loc_a101
 TRUNCATE TABLE silver.erp_loc_a101;
 insert into silver.erp_loc_a101 (cid,cntry)
 SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE
        WHEN cleaned_cntry = 'DE' THEN 'Germany'
        WHEN cleaned_cntry IN ('US', 'USA') THEN 'United States'
        WHEN cleaned_cntry IS NULL OR cleaned_cntry = '' THEN 'n/a'
        ELSE cleaned_cntry
    END AS cntry
FROM (
    SELECT
        cid,
        UPPER(
            TRIM(
                REPLACE(
                    REPLACE(cntry, CHAR(13), ''),
                CHAR(10), '')
            )
        ) AS cleaned_cntry
    FROM bronze.erp_loc_a101
) t;


 -- Inserting data into silver.erp_px_cat_g1v2
 TRUNCATE TABLE silver.erp_px_cat_g1v2;
 insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
 select id,cat,subcat,maintenance from bronze.erp_px_cat_g1v2


 
  
