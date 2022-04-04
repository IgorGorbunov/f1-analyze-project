{{ config(materialized='table', cluster_by = 'year') }}

with constr_year_results as (
SELECT distinct c.name, 
  sum(points) over (partition by c.name, r.year) as constructor_result_points, 
  r.year
FROM {{ source('stage', 'external_table_constructor_results') }} cr
inner join {{ source('stage', 'external_table_races') }} r
  on r.raceid = cr.raceid
inner join {{ source('stage', 'external_table_constructors') }} c
  on c.constructorid = cr.constructorid 
)
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
from constr_year_results