-- DATABASE FROM 'https://github.com/AlexTheAnalyst/MySQL-YouTube-Series/blob/main/layoffs.csv'

-- Data cleaning	
USE layoff_dbms;

DROP TABLE IF EXISTS layoffs;

CREATE TABLE layoffs(
	company text,
    location text,
    industry text,
    total_laid_off int DEFAULT NULL,
    percentage_laid_off text,
    date text,
    stage text,
    country text,
    funds_raised_millions int DEFAULT NULL
    );

SELECT * FROM layoffs;

-- WE can see there are so much of 'NULL', and '0' values instead of Default Null values in mysql so 
-- We are going to change 'Null', and '0' to default Null values in mysql
-- To disbale the safe mode in sql to update tables
SET SQL_SAFE_UPDATES = 0;
-- REPLACE 'NULL', and 0 WITH DEFAULT NULL VALUE
UPDATE layoffs SET total_laid_off = NULL WHERE total_laid_off=0; 
UPDATE layoffs SET percentage_laid_off = NULL WHERE percentage_laid_off='NULL'; 
UPDATE layoffs SET funds_raised_millions = NULL WHERE funds_raised_millions=0; 


-- 1. Remove Duplicates
-- 2. Standarise the Data
-- 3. Null values or blank values
-- 4. Remove any Columns 

-- Create table layoffs_staging which have the same columns with layoffs
DROP TABLE IF EXISTS layoffs_staging;

-- Create table layoffs_staging with all same columns from layoffs table
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

-- Insert values to layoffs_staging from layoffs
INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- Partition those selected columns by which has the same values and make row_num columns which has the row number by asc order
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging;

-- To check the duplicates in the database by looking at the row_num column
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, date, stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;










