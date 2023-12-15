with t1 as (
  select 
     card,
     summ,
     first_value(to_char(datetime, 'yyyy-mm')) over(partition by card order by datetime) as cohort,
     extract(days from datetime - first_value(datetime) over(partition by card order by datetime)) as diff
  from bonuscheques
  where card like '2000%'
)
select
  cohort,
  count(distinct card) as cnt,
  max(diff) as max_diff,
  sum(case when diff = 0 then summ end) / count(distinct card) as "0",
  case when max(diff) > 0 then sum(case when diff <= 30 then summ end) / count(distinct card) end as "30",
  case when max(diff) > 30 then sum(case when diff <= 60 then summ end) / count(distinct card) end as "60",
  case when max(diff) > 60 then sum(case when diff <= 90 then summ end) / count(distinct card) end as "90",
  case when max(diff) > 90 then sum(case when diff <= 180 then summ end) / count(distinct card) end as "180",
  case when max(diff) > 180 then sum(case when diff <= 300 then summ end) / count(distinct card) end as "300"
from t1
group by cohort
order by cohort
