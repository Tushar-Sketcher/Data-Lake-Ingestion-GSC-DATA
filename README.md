# Data-Lake-Ingestion-GSC-DATA
### Summary
Serverless-gsc-data-ingestor uses GSC API to fetch various combinations of metric queries & automate the process of data ingestion & make a daily snapshot of SEO metric accessible through data lake. This data is then processed by the dag datalake_gsc_daily DAG and stored the result into the datalake S3 location.


### Goals & Requirements

Requirements: DAG should process the raw data generated from serverless-gsc-data-ingestor and stored the result into data lake which can be access through hive table.

Goal: Traffic team needs transformed data to make decisions more efficiently.
