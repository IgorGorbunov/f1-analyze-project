# f1-analyze-project
Project for analyze f1 history results


# Todo list

1. Create GCP environment
2. Deploy DCP environment with Terraform
3. Create local airflow dag, putting files into BigQuery
4. Deploy airflow on GCP
5. Try to create some dashs
6. Add transformations
7. Create good dashs

# Score goal
**Problem description**
2 points: Problem is well described and it's clear what the problem the project solves
**Cloud**
4 points: The project is developed on the clound and IaC tools are used
**Data ingestion (choose either batch or stream)**
Batch / Workflow orchestration
4 points: End-to-end pipeline: multiple steps in the DAG, uploading data to data lake
**Data warehouse**
2 points: Tables are created in DWH, but not optimized
4 points: Tables are partitioned and clustered in a way that makes sense for the upstream queries (with explanation)
**Transformations (dbt, spark, etc)**
2 points: Simple SQL transformation (no dbt or similar tools)
4 points: Tranformations are defined with dbt, Spark or similar technologies
**Dashboard**
4 points: A dashboard with 2 tiles
**Reproducibility**
4 points: Instructions are clear, it's easy to run the code, and the code works

**Total Score: 22 / 26**

# Deploy
