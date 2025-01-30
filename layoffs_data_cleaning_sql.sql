CREATE TABLE `layoffs_staging3` (
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


insert into layoffs_staging3
with cte as(
select *,
row_number() over (partition by company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions) as row_num
from layoffs_staging)
select * from cte;

delete from layoffs_staging3
where row_num>1;

-- standardizing data

select distinct(trim(company)) 
from layoffs_staging3;

update layoffs_staging3
set company=trim(company);

select distinct industry
from layoffs_staging3
order by industry;

update layoffs_staging3
set industry="Crypto Currency"
where industry="Crypto";

select distinct location 
from layoffs_staging3
order by location;

select distinct country,trim(trailing "." from country)
from layoffs_staging2
order by country;

update layoffs_staging3
set country=trim(trailing"." from country)
where country like "United states%";

select * from layoffs_staging3
where country ="United States";

select  `date`,
str_to_date(`date`, "%m/%d/%Y")
from layoffs_staging3;

update layoffs_staging3
set `date` =str_to_date(date, "%m/%d/%Y");


select * from layoffs_staging3;

SELECT 
    *
FROM
    layoffs_staging2 AS t1
        JOIN
    layoffs_staging2 AS t2 ON t1.company = t2.company
        AND t1.location = t2.location
where  t1.industry ="null"; 

select *
from layoffs_staging3
where industry is null
or industry="";

select *
from layoffs_staging3 as t1
join layoffs_staging3 as t2
on t1.company=t2.company
and t1.location=t2.location
where t1.industry is null;

update layoffs_staging3
set industry=null
where industry="";


select t1.industry,t2.industry
from layoffs_staging3 as t1
join layoffs_staging3 as t2
on t1.company=t2.company
where t1.industry =t2.industry;

update layoffs_staging3  t1
JOIN layoffs_staging3 t2 
    ON t1.company = t2.company
set t1.industry=t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_staging3
where company="Bally's Interactive";

select *
from layoffs_staging3
where company ="Airbnb";


select *
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging3;

alter table layoffs_staging3
drop column row_num;