

  create or replace view `zoomcampproject`.`dbt_igorbunov`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `zoomcampproject`.`dbt_igorbunov`.`my_first_dbt_model`
where id = 1;

