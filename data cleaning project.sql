-- DATA CLEANING  --

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

# 2.Standardized the Data

select company,trim(company)from layoffs_staging2;

select distinct industry from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

SET SQL_SAFE_UPDATES = 0;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry="Crypto"
where industry like "Crypto%";

select distinct country
 from layoffs_staging2
 order by 1;
 
 select country
 from layoffs_staging2 where country like "United States%"
 order by 1;
 
 select distinct country, trim(trailing '.' from country)
 from layoffs_staging2
 order by 1;
 
update layoffs_staging2
set country= trim(trailing '.' from country)
where country like "United States%";

select  distinct country from layoffs_staging2;

select date,
str_to_date(date,'%m/%d/%Y')
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

select date from layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


------------------------------------------------

 # 3.remove null values and blank values
 
 select * from 
 layoffs_staging2 where  total_laid_off is null and
 percentage_laid_off is null;
 
 select * from layoffs_staging2 
 where industry is null or  industry="" ;

select * 
from layoffs_staging2
where company="Airbnb";

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company=t2.company
   and t1.location=t2.location
where (t1.industry is null or t1.industry ="")
and t2.industry is not null;

update layoffs_staging2
set industry =Null
where industry="";

 update layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company=t2.company
   set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

 select * from 
 layoffs_staging2 where  total_laid_off is null and
 percentage_laid_off is null;

delete from
 layoffs_staging2 where  total_laid_off is null and
 percentage_laid_off is null;
 
 select * from layoffs_staging2;
 
 -----------------------------------------------------------------------
#   4.Remove any Column data

 select * from layoffs_staging2;
 
 alter table layoffs_staging2
 drop column row_num;








