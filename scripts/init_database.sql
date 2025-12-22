/*
=============================================================
Création de la base de données et des schémas
=============================================================
Objectif du script :
    Ce script crée une nouvelle base de données nommée « DataWarehouse » après avoir vérifié
    si elle existe déjà.
    Si la base de données existe, elle est supprimée puis recréée.
    En complément, le script met en place trois schémas au sein de la base :
    « bronze », « silver » et « gold ».

AVERTISSEMENT :
    L’exécution de ce script supprimera entièrement la base de données « DataWarehouse »
    si elle existe.
    Toutes les données contenues dans la base seront définitivement perdues.
    Procédez avec précaution et assurez-vous de disposer de sauvegardes appropriées
    avant d’exécuter ce script.
*/

USE master; -- On switch sur master pour pouvoir créer d'autres bases de données

GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Création de la base de données 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
--Le "GO" est comme un séparateur, il indique à SQL d'exécuter complétement la 1ere commande avant de passer à la suivante
--Pour voir les schemas : DataWarehouse ==> Security ==> Schema

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
