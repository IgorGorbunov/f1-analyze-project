

  create or replace table `zoomcampproject`.`dbt_igorbunov`.`f1_mart`
  
  cluster by year
  OPTIONS()
  as (
    

select name, year, difference, 
  constructor_result_points, difference*1.0/constructor_result_points as prc_own_points, 
  year_points_sum, difference*1.0/year_points_sum as prc_all_points, 
  two_constr_points_sum, difference*1.0/two_constr_points_sum as prc_top2_points
from `zoomcampproject`.`dbt_igorbunov`.`f1_stage`
where place = 1
  );
  