{{ config(materialized='table') }}

with dates as (
  select dateadd(day, seq4(), to_date('2000-01-01')) as calendar_date
  from table(generator(rowcount => 36525)) 
)
select
  row_number() over(order by calendar_date) as date_sk,
  calendar_date,
  year(calendar_date)                        as year,
  month(calendar_date)                       as month,
  day(calendar_date)                         as day,
  to_char(calendar_date,'Mon')               as month_name,
  to_char(calendar_date,'DY')                as day_of_week,
  weekofyear(calendar_date)                  as week,
  last_day(calendar_date) = calendar_date    as is_month_end
from dates
