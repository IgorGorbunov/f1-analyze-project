

  create or replace table `zoomcampproject`.`dbt_igorbunov`.`f1_stage`
  
  
  OPTIONS()
  as (
    

select * from `zoomcampproject`.`f1_data_all`.`external_table_constructor_results`
limit 100
  );
  