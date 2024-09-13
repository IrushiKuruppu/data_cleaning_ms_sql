-- DATA CLEANING

-- checking data
SELECT * FROM layoffs
 
-- 1. remove duplicates
-- 2. standerdize the data
-- 3. null values or blanck values 
-- 4. remove columns 
-- 5. 

-- copying all the data from the raw data table. 
-- to make sure we do not do any harm to raw data. 
/* mysql
CREATE TABLE layoffs_staging
LIKE layoffs */


SELECT * 
INTO layoffs_staging
FROM layoffs
-- this line is useful when you want to copy the table without any records -> WHERE 0 = 1

SELECT * 
FROM layoffs_staging

-- find duplicates 

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off,'date' ORDER BY company) AS row_num
FROM layoffs_staging;

-- create CTE 
WITH duplicates AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country, funds_raised_millions ORDER BY company) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicates
WHERE row_num > 1

-- rechecking randomly 
SELECT * 
FROM 

