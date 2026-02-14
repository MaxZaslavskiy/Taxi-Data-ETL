IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TaxiDb')
BEGIN
    CREATE DATABASE [TaxiDb];
END
GO

USE [TaxiDb];
GO