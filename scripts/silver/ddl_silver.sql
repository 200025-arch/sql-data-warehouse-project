/*
===============================================================================
Script DDL : Création des tables de la couche Silver
===============================================================================
Objectif :
    Ce script crée les tables dans le schéma « silver » en supprimant
    les tables existantes si elles sont déjà présentes.
    Exécutez ce script pour redéfinir la structure DDL des tables
    de la couche « silver ».
===============================================================================
*/


USE DataWarehouse

--Création de tables (Source CRM)


--Si la table existe, on va la drop (supprimer) puis la recréer.
IF OBJECT_ID('sliver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE sliver.crm_cust_info;
GO

--Création de la table "crm_cust_info"
CREATE TABLE sliver.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()

);

--Si la table existe, on va la drop (supprimer) puis la recréer.
IF OBJECT_ID('sliver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE sliver.crm_prd_info;
GO
--Création de la table "crm_prd_info"
CREATE TABLE sliver.crm_prd_info (
prd_id INT,
prd_key NVARCHAR (50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()

);


IF OBJECT_ID('sliver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE sliver.crm_sales_details;
GO
--Création de la table "sales_details"
CREATE TABLE sliver.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()

);

--Création de tables (Source ERP)

IF OBJECT_ID('sliver.erp_CUST_AZ12', 'U') IS NOT NULL
    DROP TABLE sliver.erp_CUST_AZ12;
GO
--Création de la table "erp_CUST_AZ12"
CREATE TABLE sliver.erp_CUST_AZ12 (
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()

);


IF OBJECT_ID('sliver.erp_LOC_A101', 'U') IS NOT NULL
    DROP TABLE sliver.erp_LOC_A101;
GO
--Création de la table "erp_LOC_A101"

CREATE TABLE sliver.erp_LOC_A101(
cid NVARCHAR(50),
cntry NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()

);


IF OBJECT_ID('sliver.erp_PX_CAT_G1V2', 'U') IS NOT NULL
    DROP TABLE sliver.erp_PX_CAT_G1V2;
GO
--Création de la table "erp_PX_CAT_G1V2"

CREATE TABLE sliver.erp_PX_CAT_G1V2 (
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()

);
