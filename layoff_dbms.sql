-- Data cleaning project
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
UPDATE layoffs SET `date` = NULL WHERE `date`='NULL'; 

-- My analyse process 
	-- 1. Remove Duplicates
	-- 2. Standarise the Data
	-- 3. Null values or blank values
	-- 4. Remove any Columns 


-- 1. Remove Duplicates

-- Drop table layoffs_staging if its already exsits
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
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised_millions) AS row_num
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

-- Drop table layoffs_staging2 if its already exists
DROP TABLE IF EXISTS layoffs_staging2; 

-- Now I am going to create table named layoffs_staging2 which we are going to delete all the duplicate rows from 
-- layoffs_staging
CREATE TABLE layoffs_staging2(
	company text,
    location text,
    industry text,
    total_laid_off int DEFAULT NULL,
    percentage_laid_off text,
    date text,
    stage text,
    country text,
    funds_raised_millions int DEFAULT NULL,
    row_num int
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Check if I got the same duplicates with layoffs_staging
SELECT * FROM layoffs_staging2
WHERE row_num > 1;

-- Insert all the rows from layoffs_staging to layoffs_staging2
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Now I am going to delete all the duplicates in table layoffs_staging2
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Check if I deleted all the duplicates from the table
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;


-- Standarising Data

-- TRIM(remove takes off the white spaces) the company names
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- UPDATE company names with TRIM(company) name
UPDATE layoffs_staging2
SET company = TRIM(company);

-- I saw there are some duplicate industries which has little difference so we are going to
-- change that name to the same name
SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Update all the industry which has Crypto at the start to Crypto
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- I can see the problem with united states. so we are going to Trim the country name
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- I can see this code could fix our problem
SELECT DISTINCT country, TRIM(trailing '.' FROM country)
FROM layoffs_staging2
WHERE country LIKE 'United States%';

-- Fix the problem
UPDATE layoffs_staging2
SET country = TRIM(trailing '.' FROM country)
WHERE country LIKE 'United States%';

-- Now I am going to change date to datetime format
-- As we can see my date column is formatted in text format
SELECT `date`
FROM layoffs_staging2;

-- Use str_to_date() function to change text format to datetime format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Now update it
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Now the date column has the datetime format so I am going to change the date's datetype to date type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- 3. NULL Values and Blank Values

-- I found there are bunch of blanks or null values in industry column 
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT * 
FROM layoffs_staging2
WHERE company LIKE 'Air%';

-- I am assuming that if there are rows having the same company are having the same industry 
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- So I will put the same industry names in the blanks or null whatever with using join table
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Seems it does not woking because industry column has blank value instead of null values
-- So I will quickly update that to null value
UPDATE layoffs_staging2
SET industry = NULL
WHERE (industry = '' OR industry = 'NULL');

-- Now fix it and check
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- See there are numerous of columns which do not have total_laid_off and percentage_laid_off values 
-- which means it is not nessecary, so I will drop every rows which do not have both of them
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete rows from layoffs_staging2
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- 4. Remove any Column

-- Now I finished finalising the table so at this point I don't need row_num columns
-- Because this table does not have any duplicates, so I will drop that column from the table
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Now it's done
SELECT * FROM layoffs_staging2;


-- Maybe I nessecarily to put values into total_laid_off, and percentage_laid_off's Null by scrapping from 
-- the websites or the articles but this is totally different things


