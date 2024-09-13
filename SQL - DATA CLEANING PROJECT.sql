-- DATA CLEANING
USE world_layoffs;
-- checking data
SELECT * FROM layoffs;

 


-- 1. remove duplicates
-- 2. standerdize the data
-- 3. null values or blanck values 
-- 4. remove columns 

-- copying all the data from the raw data table. 
-- to make sure we do not do any harm to raw data. 

SELECT * 
INTO layoffs_staging
FROM layoffs;
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
FROM layoffs_staging
WHERE company = 'casper'

-- delete duplicates while checking and within the CTE
WITH duplicates_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country, funds_raised_millions ORDER BY company) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicates_cte
WHERE row_num > 1;

-- finally we have removed all the duplicates.  

-- 2nd step - standardizing data. 

-- remove unnessary white spaces from company column.
UPDATE layoffs_staging
SET company = TRIM(company);

-- checking industry column 
SELECT DISTINCT industry
FROM layoffs_staging
ORDER BY industry

-- too many crypto can be found even after duplicates. Now will standardize. 
SELECT * 
FROM layoffs_staging
WHERE industry LIKE 'Crypto%'

-- now standadize all crypto's into one unique way which is "Crypto"
UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'

-- DONT FORGET THE EMPTY VALUES IN INDUSTRY

-- checking location column 
SELECT DISTINCT location
FROM layoffs_staging
ORDER BY location

-- I saw some few issues like Florianópolis, Düsseldorf/ Dusseldorf ,Malmö so orderly which has to be Florian�polis, D�sseldorf, Malmo when I check on internet
UPDATE layoffs_staging
SET location = 'Florian�polis'
WHERE location = 'Florianópolis';

UPDATE layoffs_staging
SET location = 'D�sseldorf'
WHERE location = 'Düsseldorf' OR location ='Dusseldorf';

UPDATE layoffs_staging
SET location = 'Malmo'
WHERE location = 'Malmö';

-- Let's look ata the country column. 
SELECT DISTINCT country
FROM layoffs_staging
ORDER BY country;

-- when you go through the list of the countries you can see that we have United states mentioned twice. 
-- now we are gonna confirm it by filtering and then trim the unnecessary parts 

SELECT DISTINCT country
FROM layoffs_staging
WHERE country LIKE 'United State%';

SELECT DISTINCT country, TRIM(TRAILING '.'FROM country)
FROM layoffs_staging
WHERE country LIKE 'United State%';

UPDATE layoffs_staging
SET country = TRIM(TRAILING '.'FROM country)
WHERE country LIKE 'United State%';

-- right now date is in text format which is not correct so we need to change the type to date.

SELECT date,
TRY_CAST(date as date) as StringToDate
FROM layoffs_staging;

SELECT date,
TRY_CAST(date as date) as StringToDate
FROM layoffs_staging;

-- updating date format
UPDATE layoffs_staging
SET date = TRY_CAST(date as date);

-- modify the column data, data type to date
ALTER TABLE layoffs_staging
ALTER COLUMN date DATE;
-- there are null data. 


-- checking all the nulls in the table 
-- is not null is not supporting here so we have to check it as a string 
-- either we can convert these string NULL text into NULL value and then continue or we can 
SELECT * 
FROM layoffs_staging
WHERE total_laid_off = 'NULL'
AND percentage_laid_off = 'NULL';

-- sometimes when they have multiple rows for the same company name and some of those
--have the company industry instead of removing the full column we can update those
SELECT *
FROM layoffs_staging
WHERE industry = 'NULL'
OR industry= '';

SELECT * 
FROM layoffs_staging
WHERE industry IS NULL;

-- self join to fillup the values
SELECT tb1.company, tb1.location, tb1.industry, tb2.company, tb2.location, tb2.industry 
FROM layoffs_staging tb1
JOIN layoffs_staging tb2 
     ON tb1.company = tb2.company
	 AND tb1.location = tb2.location
WHERE (tb1.industry = 'NULL' OR tb1.industry = '')
AND tb2.industry != 'NULL'
ORDER BY tb1.company;

UPDATE layoffs_staging
SET industry = NULL
WHERE industry = 'NULL' OR industry = '';


UPDATE tb1
SET tb1.industry = tb2.industry
FROM layoffs_staging AS tb1
JOIN layoffs_staging AS tb2 
     ON tb1.company = tb2.company
WHERE tb1.industry IS NULL
  AND tb2.industry IS NOT NULL;

  SELECT tb1.company, tb1.location, tb1.industry, tb2.company, tb2.location, tb2.industry 
FROM layoffs_staging tb1
JOIN layoffs_staging tb2 
     ON tb1.company = tb2.company
	 AND tb1.location = tb2.location
WHERE tb1.industry IS NULL
  AND tb2.industry IS NOT NULL
ORDER BY tb1.company;

SELECT * FROM layoffs_staging;

-- Now we are back on the total laid off and percentage lad off null values. 

-- first I would convert string null to NUlL
UPDATE layoffs_staging
SET total_laid_off = NULL
WHERE total_laid_off = 'NULL';

UPDATE layoffs_staging
SET percentage_laid_off = NULL
WHERE percentage_laid_off = 'NULL';

DELETE 
FROM layoffs_staging
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

