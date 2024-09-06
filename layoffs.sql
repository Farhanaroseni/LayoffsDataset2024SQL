-- Create table to import table from csv file

create table layoffs(
	company varchar(250),
	locations varchar(250) ,
	industry varchar(250),
	total_laid_off int,
	percentage_laid_off numeric,
	dates date,
	stage varchar(50),
	country varchar(50),
	funds_raised numeric
);

-- Table imported

select * from layoffs;

-- Data cleaning

-- Create a staging data so that we are not overwrite the original data in case of anything.
drop table layoffs_staging;
	
create table layoffs_staging as
select * from layoffs; 

-- Add a unique identifier for easy to work later on

ALTER TABLE layoffs_staging 
ADD COLUMN id SERIAL PRIMARY KEY;

-- From here on we will be only using this staging table to clean the data

select * from layoffs_staging;

------------------- Remove duplicates----------------------------

with duplicate_cte as (
select *,
row_number() over(partition by company, locations, industry, total_laid_off, 
	percentage_laid_off, dates, stage, country, funds_raised) as row_num
from layoffs_staging
)
delete from layoffs_staging
where id in (
	select id
	from duplicate_cte
	where row_num > 1
);

--2 duplicate rows was removed.

select * from layoffs_staging;

-- Standardizing data

select company, trim(company) 
from layoffs_staging;

update layoffs_staging
set company = trim(company);

select distinct industry
from layoffs_staging
order by 1;

select * from layoffs_staging
where company = 'eBay';

update layoffs_staging
set industry = 'Retail'
where industry like 'https%' and company = 'eBay';

select distinct locations
from layoffs_staging
order by 1;

select distinct country
from layoffs_staging
order by 1;



select *
from layoffs_staging
where industry is null or industry = ' ';

--Check whether there is the same company in other rows so that we can populate the industry if there is.
select *
from layoffs_staging
where company = 'Appsmith';



select * 
from layoffs_staging
where (total_laid_off is null or  and percentage_laid_off is null;

delete 
from layoffs_staging
where total_laid_off is null and percentage_laid_off is null;


-- Exploratory Data Analysis

select *
from layoffs_staging;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging;

select *
from layoffs_staging
where percentage_laid_off = 1
order by total_laid_off desc ;

select *
from layoffs_staging
where percentage_laid_off = 1
order by funds_raised desc ;

select min(dates), max(dates)
from layoffs_staging;

select company, sum(total_laid_off)
from layoffs_staging
group by company
order by 2 desc;

select industry, sum(total_laid_off)
from layoffs_staging
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs_staging
group by country
order by 2 desc;

select dates, sum(total_laid_off)
from layoffs_staging
group by dates
order by 2 desc;

select extract (year from dates), sum(total_laid_off)
from layoffs_staging
group by extract (year from dates)
order by 2 desc;

select stage, sum(total_laid_off)
from layoffs_staging
group by stage
order by 2 desc;

select to_char(dates, 'YYYY-MM'), sum(total_laid_off)
from layoffs_staging
group by to_char(dates, 'YYYY-MM')
order by 1 asc;


with rolling_total as (
select to_char(dates, 'YYYY-MM') as months, sum(total_laid_off) as total_laid
from layoffs_staging
group by to_char(dates, 'YYYY-MM')
order by 1 asc
)
select months, total_laid, sum(total_laid) over(order by months) as rolling_total
from rolling_total;


select company, extract (year from dates), sum(total_laid_off)
from layoffs_staging
group by company, extract (year from dates)
order by 3 desc;

with company_year (company, years, total_laid_off) as (
	select company, extract (year from dates), sum(total_laid_off) 
	from layoffs_staging
	where total_laid_off is not null
	group by company, extract (year from dates)
), company_year_rank as (
select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
)
select *
from company_year_rank
where ranking <= 5
