/*
===============================================================================
Contrôles de qualité
===============================================================================
Objectif du script :
    Ce script effectue différents contrôles de qualité afin de vérifier la
    cohérence, l’exactitude et la standardisation des données dans la couche
    « silver ». Il inclut notamment des vérifications sur :
        - Les clés primaires nulles ou dupliquées.
        - Les espaces indésirables dans les champs de type chaîne de caractères.
        - La standardisation et la cohérence des données.
        - Les plages de dates invalides et les incohérences d’ordre chronologique.
        - La cohérence des données entre champs liés.

Notes d’utilisation :
    - Exécuter ces contrôles après le chargement des données dans la couche Silver.
    - Analyser et corriger toute anomalie détectée lors des contrôles.
===============================================================================
*/


/* bronze.crm_cust_info */

SELECT
	*
FROM bronze.crm_cust_info

-- Pour vérifier les doublons dans PK pour la table "bronze.crm_cust_info"

SELECT 
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


--Vérifier les espace indésirables

--firstname
SELECT
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--lastname

SELECT
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT *  FROM bronze.crm_cust_info

--Data Standardization & consistency

--Sur le genre
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

SELECT 
	CASE 
		WHEN cst_gndr = 'F' THEN 'Female'
		WHEN cst_gndr = 'M' THEN 'Male'
		ELSE 'None'
	END cst_gndr

FROM bronze.crm_cust_info

--Sur le statut
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info

SELECT 
	CASE 
		WHEN cst_marital_status = 'S' THEN 'Single'
		WHEN cst_marital_status = 'M' THEN 'Maried'
		ELSE 'None'
	END cst_marital_status
FROM bronze.crm_cust_info

SELECT *  FROM Sliver.crm_cust_info

/* bronze.crm_prd_info */

SELECT * FROM bronze.crm_prd_info


--Vérifier les doublons et les NULL
SELECT
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--Vérifier les espace vident sur "prd_key"

SELECT
	prd_key
FROM bronze.crm_prd_info
WHERE prd_key != TRIM(prd_key)

--Dériver des colonnes à partir de prd_key ==> cat_id
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_cat_key,
	SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key
FROM bronze.crm_prd_info


--Vérifier les espace vident sur "prd_nm"

SELECT * FROM bronze.crm_prd_info

SELECT 
	prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


--Vérifier les NULLs et valeurs négatives sur la colonne "prd_cost"

SELECT
	prd_id,
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

SELECT
	prd_id,
	COALESCE(prd_cost, 0) AS prd_cost
FROM bronze.crm_prd_info

--Vérifier les valeurs de la colonne "prd_line"
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

SELECT
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END prd_line
FROM bronze.crm_prd_info

--Vérification sur les dates

--La date de fin est plus petite que la date de début
SELECT
	*
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt


SELECT
	prd_id,
	prd_key,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS test_recup, --Récup Date
	DATEADD(day, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) prd_end_dt_test --Retirer 1j pour éviter le chevauchement
FROM bronze.crm_prd_info
WHERE prd_key = 'AC-HE-HL-U509'

/* bronze.crm_sales_details */

SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
--WHERE sls_cust_id NOT IN (SELECT cst_id FROM sliver.crm_cust_info)
--WHERE sls_prd_key NOT IN (SELECT prd_key FROM sliver.crm_prd_info)

--Check des espaces vides sur "sls_ord_num"
SELECT
	sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

--Check sur Les dates : <= 0 & longueur != 8 (yyyymmdd)
--On applique la même logique sur les autres colonnes de date
SELECT
	NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8;

--Vérifier s'il y'a des cellules où sls_order_dt  > sls_ship_dt | sls_due_dt
SELECT
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--Check sur la colonne "sls_sales"

SELECT
	sls_sales,
	sls_quantity,
	sls_price,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END test_sales,
	CASE
		WHEN sls_price IS NULL OR sls_price <=0 THEN ABS(sls_sales) / sls_quantity
		ELSE sls_price
	END test_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price OR
sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


--Check après corrections & insertions dans sliver.crm_sales_details
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM sliver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price OR
sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


/* Source ERP */

--Table sliver.erp_CUST_AZ12

SELECT
	*
FROM bronze.erp_CUST_AZ12
WHERE cid NOT IN (SELECT cst_key FROM sliver.crm_cust_info)

SELECT * FROM sliver.crm_cust_info;

--Check sur la date
SELECT
	bdate
FROM bronze.erp_CUST_AZ12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

--Valeurs de la colonne gen

SELECT DISTINCT 
	gen,
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END gen_test
FROM bronze.erp_CUST_AZ12

--Table sliver.erp_LOC_A101

SELECT
	cid,
	REPLACE(cid, '-', '') AS test,
	cntry
FROM bronze.erp_LOC_A101
--WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM sliver.crm_cust_info)

SELECT * FROM sliver.crm_cust_info;


--Les pays
SELECT DISTINCT 
	cntry,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
		WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
		ELSE TRIM(cntry)
	END cntry_test
FROM bronze.erp_LOC_A101

--Table sliver.erp_PX_CAT_G1V2

--Check sur l'id 
SELECT
	*
FROM bronze.erp_PX_CAT_G1V2
WHERE id NOT IN (SELECT cat_id FROM sliver.crm_prd_info)

SELECT * FROM sliver.crm_prd_info

--Check sur la colonne "cat"
SELECT DISTINCT cat
FROM bronze.erp_PX_CAT_G1V2

SELECT * FROM bronze.erp_PX_CAT_G1V2
WHERE cat != TRIM(cat)

--Check sur la colonne subcat
SELECT DISTINCT subcat
FROM bronze.erp_PX_CAT_G1V2

SELECT * FROM bronze.erp_PX_CAT_G1V2
WHERE subcat != TRIM(subcat)

--Check sur la colonne "maintenance"
SELECT DISTINCT maintenance
FROM bronze.erp_PX_CAT_G1V2

SELECT * FROM bronze.erp_PX_CAT_G1V2
WHERE maintenance != TRIM(maintenance)

EXEC sliver.silver_load
