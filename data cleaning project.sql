-- DATA CLEANING  --

-- Dataset : https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- 1. Remove the Duplicates
-- 2.Standardized the Data
-- 3.null values and blank values
-- 4.Remove any Column data

# 1.Remove the Duplicate values

-- Step 1: View the original data in the 'layoffs' table
SELECT * FROM layoffs;

-- Step 2: Create a copy of the 'layoffs' table for cleaning.
-- This ensures the original data remains untouched while we clean the copy.

CREATE TABLE layoffs_staging
LIKE layoffs;

-- Step 3: Insert all the data from 'layoffs' into the new staging table.

INSERT INTO layoffs_staging
SELECT * FROM layoffs;

-- Step 4: Confirm the data has been copied correctly into 'layoffs_staging'

SELECT * FROM layoffs_staging;

-- Step 5: Use the ROW_NUMBER() function to find duplicate rows.
-- ROW_NUMBER() assigns a unique row number to each row within a partition.

SELECT *, 
ROW_NUMBER() OVER (
  PARTITION BY company, industry, total_laid_off, percentage_laid_off, "date"
) AS row_num
FROM layoffs_staging;

-- Step 6: Use a Common Table Expression (CTE) to generate row numbers and identify duplicates.
-- This time, the grouping includes more columns for more accurate duplication detection.

WITH duplicate_cte AS (
  SELECT *, 
  ROW_NUMBER() OVER (
    PARTITION BY company, location, total_laid_off, percentage_laid_off, "date",
                 stage, country, funds_raised_millions
  ) AS row_num
  FROM layoffs_staging
)

-- View all rows that have duplicates (i.e., where row_num > 1).

SELECT * FROM duplicate_cte
WHERE row_num > 1;

-- Step 7: Example check – view records for a specific company ('Casper')

SELECT * FROM layoffs_staging
WHERE company = 'Casper';

-- Step 8: Attempt to delete duplicates using a CTE (This will not work in MySQL!)
-- MySQL does not allow DELETE directly from a CTE.

WITH duplicate_cte AS (
  SELECT *, 
  ROW_NUMBER() OVER (
    PARTITION BY company, location, total_laid_off, percentage_laid_off, "date",
                 stage, country, funds_raised_millions
  ) AS row_num
  FROM layoffs_staging
)
DELETE FROM duplicate_cte
WHERE row_num > 1;  -- This will throw an error because MySQL doesn’t allow deleting from a CTE.

-- COMMENT: The ROW_NUMBER() function is used to assign unique numbers to rows.
-- When duplicate values exist, it assigns different numbers to each duplicate row.
-- We can then use this to filter out or delete duplicates.

-----------------------------------------------------------------------------------

-- SOLUTION TO DELETE DUPLICATES SAFELY IN MYSQL --

-- Step 9: Create a new table 'layoffs_staging2' that includes a 'row_num' column.
-- This is necessary because we can't delete directly using ROW_NUMBER() in MySQL.
-- So we create a new table with the row numbers already included.

CREATE TABLE layoffs_staging2 (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT DEFAULT NULL,
  percentage_laid_off TEXT,
  date TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT DEFAULT NULL,
  row_num INT
);

-- Step 10: Insert data into 'layoffs_staging2' from 'layoffs_staging',

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER (
  PARTITION BY company, location, total_laid_off, percentage_laid_off, "date",
               stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;

-- COMMENT:
-- Instead of trying to delete from a CTE, we insert the data with row numbers into a new table.
-- Now we can easily delete the duplicate rows using the 'row_num' column.

-- Step 11: Delete all rows from 'layoffs_staging2' where row_num > 1 (i.e., duplicates).
DELETE FROM layoffs_staging2 
WHERE row_num > 1;

-- Step 12: Confirm that duplicates have been deleted successfully.
-- This should return an empty result if all duplicates were removed.
SELECT * FROM layoffs_staging2 
WHERE row_num > 1;

-- Step 13: Disable safe update mode if necessary.
-- Safe update mode prevents accidental DELETE or UPDATE operations

SET SQL_SAFE_UPDATES = 0;
 
-----------------------------------------------------------------------------------------

-- 2. STANDARDIZE THE DATA
-- Goal: make sure text data is consistent, clean, and in the right format
------------------------------------------------

-- See current company names and how they look after removing extra spaces
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Check all unique industry names (to spot inconsistent spellings)
SELECT DISTINCT industry
FROM layoffs_staging2;

-- Remove leading and trailing spaces from all company names
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Turn off safe updates so we can run UPDATE without strict WHERE clauses
SET SQL_SAFE_UPDATES = 0;

-- Show all rows where industry starts with 'Crypto' (e.g., Cryptocurrency)
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Change all variations like 'Cryptocurrency' or 'Crypto - Web3' to just 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Check all distinct country names
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- See all country names that start with 'United States'
SELECT country
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

-- Preview what the country column would look like without a trailing dot
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- Remove any trailing '.' in country values that start with 'United States'
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Recheck the unique country list after update
SELECT DISTINCT country
FROM layoffs_staging2;

-- Preview date conversion: from text format (MM/DD/YYYY) to MySQL DATE
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Update the date column to proper DATE values using STR_TO_DATE
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Check the date column after conversion
SELECT `date`
FROM layoffs_staging2;

-- Change the column type to DATE so MySQL treats it as a real date
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


------------------------------------------------
-- 3. REMOVE NULL VALUES AND BLANK VALUES
------------------------------------------------

-- Find rows where both total_laid_off and percentage_laid_off are NULL
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Find rows where industry is missing (either NULL or empty string)
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
   OR industry = '';

-- Look at all records for Airbnb (just an example check)
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Find pairs of rows with same company & location where one has industry missing and the other has it filled
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
 AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
  AND t2.industry IS NOT NULL;

-- Change empty string industry values to actual NULL values
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Fill NULL industry values by copying from other rows with the same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Recheck rows where both total_laid_off and percentage_laid_off are NULL
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Delete rows where both total_laid_off and percentage_laid_off are NULL
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Final check of the table after deletion
SELECT *
FROM layoffs_staging2;


------------------------------------------------
-- 4. REMOVE ANY UNNEEDED COLUMNS
------------------------------------------------

-- View the table structure and data before removing the column
SELECT *
FROM layoffs_staging2;

-- Remove the 'row_num' column (likely used earlier for removing duplicates)
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;







