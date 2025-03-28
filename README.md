# GreenplumClickhouseETL
This repository contains a project to automate the processes of uploading and processing data for analytics and building data storefronts.
Основные компоненты проекта:

Data Upload: Using PXF and gpfdist to upload data to Greenplum.

Data processing: Implementation of data positioning and distribution in tables. 

Functions: Create functions for uploading data and calculating data storefronts. 

Integration with ClickHouse: Uploading data to ClickHouse for fast analytics. 

Visualization: Building a dashboard for data visualization. 

Automation: Full automation of processes using Apache Air flow, including a DAG for managing data loading and processing. 

Technologies:

Greenplum
ClickHouse
Apache Airflow
SuperSet
PXF, gpfdist
