-- Exploratory Data Analysis

select *
from layoffs_staging2;

select min(`date`), max(`date`)
from layoffs_staging2;
# Data set from 11/03/2020 - 03/06/2023, establishing timeframe

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by sum(total_laid_off) desc
;

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by sum(total_laid_off) desc
;
# Consumer, retail and transportation 3 biggest affected industries

select year (`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by year (`date`) desc
;
# Displays the trend of an incremental increase in the number of layoffs year on year

select substring(`date`, 1, 7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`, 1 ,7) is not null
group by `month`
order by 1 asc;
# This provides us with a month by month total of layoffs

with rolling_total as
(
select substring(`date`, 1, 7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`, 1 ,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off , sum(total_off) over (order by `month`) as total_incremental
from rolling_total
;
# Provides a rolling total of the monthly layoffs

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

with Company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
),
Company_year_rank as
(
select *, dense_rank () over (partition by years order by total_laid_off desc) as ranking
from Company_year
where years is not null
and total_laid_off is not null)
select * from Company_year_rank
;
# This ranks the layofss per year, per company using 2 CTEs

