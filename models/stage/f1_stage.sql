{{ config(materialized='table') }}

select * from {{ source('stage', 'external_table_constructor_results') }}
limit 100