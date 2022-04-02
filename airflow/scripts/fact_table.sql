CREATE OR REPLACE TABLE f1_data_all.fact_table AS
with constr_year_results as (
SELECT distinct c.name, 
  sum(points) over (partition by c.name, r.year) as constructor_result_points, 
  r.year
FROM f1_data_all.external_table_constructor_results cr
inner join f1_data_all.external_table_races r
  on r.raceid = cr.raceid
inner join f1_data_all.external_table_constructors c
  on c.constructorid = cr.constructorid 
)
, dds as (
select name, 
  constructor_result_points, 
  year,
  rank() over (partition by year order by constructor_result_points desc) as place,
  constructor_result_points - (lead(constructor_result_points) over 
                (partition by year order by constructor_result_points desc)) as difference,
  sum(constructor_result_points) over (partition by year) as year_points_sum,
  sum(constructor_result_points) over (partition by year 
                    order by constructor_result_points desc 
                    rows between current row and 1 following) as two_constr_points_sum
from constr_year_results)
select name, year, difference, 
  constructor_result_points, difference*1.0/constructor_result_points as prc_own_points, 
  year_points_sum, difference*1.0/year_points_sum as prc_all_points, 
  two_constr_points_sum, difference*1.0/two_constr_points_sum as prc_top2_points
from dds
where place = 1
order by year
