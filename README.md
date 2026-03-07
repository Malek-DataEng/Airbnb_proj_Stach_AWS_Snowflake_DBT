# Airbnb_proj_Stach_AWS_Snowflake_DBT

CI/CD pipeline implemented with GitHub Actions.

CI:
- dbt debug
- dbt tests
- SQL model validation

CD:
- Event-driven pipeline triggered by Snowflake tasks
- dbt transformations executed only when new data arrives
