/*
===============================================================================
Script DDL – Tables de la couche Bronze
===============================================================================
Objectif :
    Ce script définit la structure DDL des tables de la couche Bronze.
    Il supprime les tables existantes si elles sont déjà présentes,
    puis les recrée afin de garantir un schéma propre et cohérent.

Utilisation :
    Exécutez ce script pour (ré)initialiser la couche Bronze
    dans le cadre de la mise en place du data warehouse
    ou lors des itérations de développement.
===============================================================================
*/


USE DataWarehouse

--Création de tables (Source CRM)


--Si la table existe, on va la drop (supprimer) puis la recréer.
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

--Création de la table "crm_cust_info"
CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE

);

--Si la table existe, on va la drop (supprimer) puis la recréer.
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO
--Création de la table "crm_prd_info"
CREATE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key NVARCHAR (50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
);


IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO
--Création de la table "sales_details"
CREATE TABLE bronze.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

--Création de tables (Source ERP)

IF OBJECT_ID('bronze.erp_CUST_AZ12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_CUST_AZ12;
GO
--Création de la table "erp_CUST_AZ12"
CREATE TABLE bronze.erp_CUST_AZ12 (
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);


IF OBJECT_ID('bronze.erp_LOC_A101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_LOC_A101;
GO
--Création de la table "erp_LOC_A101"

CREATE TABLE bronze.erp_LOC_A101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);


IF OBJECT_ID('bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_PX_CAT_G1V2;
GO
--Création de la table "erp_PX_CAT_G1V2"

CREATE TABLE bronze.erp_PX_CAT_G1V2 (
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);
