/*
===============================================================================
Procédure stockée : Chargement de la couche Silver (Bronze → Silver)
===============================================================================
Objectif du script :
    Cette procédure stockée réalise le processus ETL (Extract, Transform, Load)
    afin d’alimenter les tables du schéma « silver » à partir du schéma « bronze ».

Actions réalisées :
    - Troncature des tables Silver.
    - Insertion des données transformées et nettoyées depuis Bronze vers les tables Silver.

Paramètres :
    Aucun.
    Cette procédure stockée n’accepte aucun paramètre et ne retourne aucune valeur.

Exemple d’utilisation :
    EXEC Silver.load_silver;
===============================================================================
*/



USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE sliver.silver_load AS

BEGIN
	
	DECLARE @start_time DATETIME, @end_time DATETIME

	BEGIN TRY

		PRINT'================================';
		PRINT'Chargement de la couche silver';
		PRINT'================================';

		PRINT'-------------------------------';
		PRINT'Chargement des tables CRM';
		PRINT'-------------------------------';
		-- Corrections erreurs sur la PK (doublons)
		/*La sous requête va grouper par "cst_id" et dans chaque fenêtre on va tier par 
		date de création (DESC) et appliquer un classement 

		Ensuite en récupère uniquement les lignes où le classement est à 1 pour ne plus avoir de doublons*/

		--En appliquant la fonction TRIM, on retire les espaces vides sur les colonnes "firstname & lastname"

		--"CASE Statement" On construit une logique conditionnelle pour remplacer les "M" et "F" par "Male & Female" pour cst_gndr
		--"CASE Statement" On construit une logique conditionnelle pour remplacer les "M" et "S" par "Maried & Single" pour cst_marital_status
		--On anticipe une potentielle modification de la case en mettant les valeurs des colonnes en majuscules (UPPER)

		--Table "sliver.crm_cust_info"
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table : sliver.crm_cust_info';
		TRUNCATE TABLE sliver.crm_cust_info

		PRINT'>> Inserting Data Into : sliver.crm_cust_info';
		INSERT INTO sliver.crm_cust_info(
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
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Maried'
				ELSE 'n/a'
			END cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC ) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1;
		SET @end_time = GETDATE();

		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'


		--Table "sliver.crm_prd_info"

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table : sliver.crm_prd_info';
		TRUNCATE TABLE sliver.crm_prd_info

		PRINT'>> Inserting Data Into : sliver.crm_prd_info';
		INSERT INTO sliver.crm_prd_info(
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
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key,
			prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line,
			prd_start_dt,
			DATEADD(day, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) prd_end_dt
		FROM bronze.crm_prd_info;

		SET @end_time = GETDATE();

		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'

		--SELECT * FROM sliver.crm_prd_info


		--Table "sliver.crm_sales_details"


		SET @start_time = GETDATE();
		PRINT'>> Truncating Table : sliver.crm_sales_details';
		TRUNCATE TABLE sliver.crm_sales_details

		PRINT'>> Inserting Data Into : sliver.crm_sales_details';
		INSERT INTO sliver.crm_sales_details (
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
			CASE
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
			--On applique la même logique pour prévenir de potentielles erreurs dans le futur
			CASE
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_ship_dt,
			CASE
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END sls_due_dt,
			CASE
				WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price <=0 THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END sls_price
		FROM bronze.crm_sales_details;

		
		SET @end_time = GETDATE();

		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'

		--SELECT * FROM sliver.crm_sales_details

		PRINT'-------------------------------';
		PRINT'Chargement des tables ERP';
		PRINT'-------------------------------';
		/* Source ERP */

		--Table sliver.erp_CUST_AZ12

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table : sliver.erp_CUST_AZ12';
		TRUNCATE TABLE sliver.erp_CUST_AZ12

		PRINT'>> Inserting Data Into : sliver.erp_CUST_AZ12';
		INSERT INTO sliver.erp_CUST_AZ12 (
		cid,
		bdate,
		gen
		)

		SELECT 
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END cid1,
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END bdate,
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END gen
		FROM bronze.erp_CUST_AZ12;
		SET @end_time = GETDATE();

		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'

		--SELECT * FROM sliver.erp_CUST_AZ12


		--Table sliver.erp_LOC_A101

		
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table : sliver.erp_LOC_A101';
		TRUNCATE TABLE sliver.erp_LOC_A101

		PRINT'>> Inserting Data Into : sliver.erp_LOC_A101';
		INSERT INTO sliver.erp_LOC_A101(
		cid,
		cntry
		)

		SELECT
			REPLACE(cid, '-', '') AS cid,
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
				WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
				ELSE TRIM(cntry)
			END cntry
		FROM bronze.erp_LOC_A101;
		SET @end_time = GETDATE();

		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'

		--SELECT * FROM sliver.erp_LOC_A101

		--Table sliver.erp_PX_CAT_G1V2

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table : sliver.erp_PX_CAT_G1V2';
		TRUNCATE TABLE sliver.erp_PX_CAT_G1V2

		PRINT'>> Inserting Data Into : sliver.erp_PX_CAT_G1V2';
		INSERT INTO sliver.erp_PX_CAT_G1V2(
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
		FROM bronze.erp_PX_CAT_G1V2;
		SET @end_time = GETDATE();

		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'

		--SELECT * FROM sliver.erp_PX_CAT_G1V2
	END TRY

	BEGIN CATCH
		PRINT'==============================================';
		PRINT'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR NUMBER' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'==============================================';
	END CATCH

END;
