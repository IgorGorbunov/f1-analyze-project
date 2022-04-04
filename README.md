# F1 analyze project
## Table of Contents
1. About the Project
2. Key files and folders explained
3. Work explained
4. Virtual environment
5. Deployment
6. Pipeline start

## 1. About the Project
## 2. Key files and folders explained
airflow - папка с эирфлоу
models - dbt
terraform
readme.md
dbt_project

## 3. Work explained
### Architecture
```mermaid
graph LR
a[Zip of CSVs] --> |Extract/Load| bb[[Google CLoud Storage]]:::cloud 
bb --> b[(BigQuery)]:::cloud 
b --> c>dbt transformation]:::cloud 
c --> b
e((Terraform)) --- b
e((Terraform)) --- bb
b --> d{{Google Data Studio}}:::cloud 

subgraph ide2 [Airflow]
a
b
bb
c
end

classDef cloud fill:#daedfe;
```
## 4. Virtual environment
## 5. Deployment
## 6. Pipeline start
