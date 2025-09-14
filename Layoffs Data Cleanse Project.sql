-- Data cleanup project

SELECT * 
FROM layoffs;

-- 1. remove duplicates
-- 2. standardize the data
-- 3. null values or blank values populated where possible
-- 4. remove unnecessary columns or rows

create table layoffs_staging
like layoffs_raw;

insert layoffs_staging
select *
from layoffs_raw;

SELECT * FROM layoffs_staging;

with cte_duplicates as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging
)
select *
from cte_duplicates
where row_num > 1
;
# from the query above we found 5 cases of duplicates. one of each of these 5 pairs need to be deleted
# to delete duplicates we must create a new table with the row_num column to filter through and delete

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
from layoffs_staging;

Delete
from layoffs_staging2
where row_num > 1
;

-- standardizing data

select company, (trim(company))
from layoffs_staging2;

update layoffs_staging2
set company = (trim(company));

select  distinct industry
from layoffs_staging2
order by 1;

#Crypto industry was also listed as cryptocurrency, the below expression standardizes them both as 'Crypto'
#United States also requires standardizing
select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%'
;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%'
;

#Formatting date column & changing date colum from a text colum to a time series column
select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2
;

Update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y')
;


alter table layoffs_staging2
modify column `date` date
;

#configuring null values

update layoffs_staging2
set industry = null
where industry = ''
;

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null)
and (t2.industry is not null)
;

update
layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null)
and t2.industry is not null;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null
;

# deleting redundant data due to lack of necessary data in multiple columns

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;
