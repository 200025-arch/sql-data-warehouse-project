/*
===============================================================================
Procédure stockée : Chargement de la couche Bronze (Source → Bronze)
===============================================================================
Objectif :
    Cette procédure stockée charge les données dans le schéma « bronze »
    à partir de fichiers CSV externes.
    Elle réalise les actions suivantes :
    - Tronque les tables de la couche bronze avant le chargement des données.
    - Utilise la commande `BULK INSERT` pour charger les données
      depuis les fichiers CSV vers les tables bronze.

Paramètres :
    Aucun.
    Cette procédure stockée n’accepte aucun paramètre
    et ne retourne aucune valeur.

Exemple d’utilisation :
    EXEC bronze.load_bronze;
===============================================================================
*/


USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME
	
	BEGIN TRY
		PRINT'================================';
		PRINT'Chargement de la couche bronze';
		PRINT'================================';

		-- Insertion en masse des données (Bulk Insert)

		PRINT'-------------------------------';
		PRINT'Chargement des tables CRM';
		PRINT'-------------------------------';

		/* Tables de la source crm */

		--Insertion dans la table "bronze.crm_cust_info"

		SET @start_time = GETDATE();
		PRINT'>>  Truncating Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info -- On vide la table et ensuite on charge les données en partant de 0 (évite les doublons)

		PRINT'>>  Inserting Data Into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\mvoum\Desktop\Boot_camp\SQL\Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, --On dit de passer la 1ere ligne (header)
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'

		--Insertion dans la table "bronze.crm_prd_info"

		SET @start_time = GETDATE();
		PRINT'>>  Truncating Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info -- On vide la table et ensuite on charge les données en partant de 0 (évite les doublons)

		PRINT'>>  Inserting Data Into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info 
		FROM 'C:\Users\mvoum\Desktop\Boot_camp\SQL\Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, --On dit de passer la 1ere ligne (header)
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'

		--Insertion dans la table "bronze.crm_sales_details"

		SET @start_time = GETDATE();
		PRINT'>>  Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details -- On vide la table et ensuite on charge les données en partant de 0 (évite les doublons)

		PRINT'>>  Inserting Data Into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details 
		FROM 'C:\Users\mvoum\Desktop\Boot_camp\SQL\Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, --On dit  de passer la 1ere ligne (header)
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'

		/* Tables de la source erp */

		PRINT'-------------------------------';
		PRINT'Chargement des tables ERP';
		PRINT'-------------------------------';

		--Insertion dans la table "bronze.erp_CUST_AZ12"

		SET @start_time = GETDATE();
		PRINT'>>  Truncating Table : bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12 -- On vide la table et ensuite on charge les données en partant de 0 (évite les doublons)

		PRINT'>>  Inserting Data Into : bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12 
		FROM 'C:\Users\mvoum\Desktop\Boot_camp\SQL\Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, --On dit  de passer la 1ere ligne (header)
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'

		--Insertion dans la table "bronze.erp_LOC_A101"
		SET @start_time = GETDATE();
		PRINT'>>  Truncating Table : bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101 -- On vide la table et ensuite on charge les données en partant de 0 (évite les doublons)

		PRINT'>>  Inserting Data Into : bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101 
		FROM 'C:\Users\mvoum\Desktop\Boot_camp\SQL\Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, --On dit  de passer la 1ere ligne (header)
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'

		--Insertion dans la table "bronze.erp_PX_CAT_G1V2"

		SET @start_time = GETDATE();
		PRINT'>>  Truncating Table : bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2 -- On vide la table et ensuite on charge les données en partant de 0 (évite les doublons)

		PRINT'>>  Inserting Data Into : bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2 
		FROM 'C:\Users\mvoum\Desktop\Boot_camp\SQL\Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, --On dit  de passer la 1ere ligne (header)
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT'>> Load durantion : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT'-----------------------'

	END TRY

	BEGIN CATCH
		PRINT'==============================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR NUMBER' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'==============================================';
	END CATCH

END;
