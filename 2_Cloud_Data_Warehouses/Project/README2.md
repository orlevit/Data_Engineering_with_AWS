# Sparkify Data Engineering Project

### Introduction

Sparkify, a music streaming startup, has experienced rapid growth in its user base and song database. To enhance scalability and analytics capabilities, Sparkify aims to migrate its data to the cloud. Currently, user activity logs are stored in JSON format in one S3 directory, and song metadata in JSON format is stored in another directory.

This project develops an ETL pipeline to extract data from S3, stage it in Amazon Redshift, and transform it into a set of dimensional tables. This structure enables Sparkify's analytics team to gain deeper insights into user listening habits.

### Project Components

This project includes three main components:

1. **Table Creation and Management**
   - **Script**: `create_table.py`
   - **Description**: Drops existing tables (if any) and creates new ones in Redshift. Ensures the schema is set up properly before any data processing.

2. **Data Loading and Transformation**
   - **Script**: `etl.py`
   - **Description**: Loads raw data from S3 into staging tables in Redshift and then transforms and inserts the data into the analytics tables following the star schema.

3. **SQL Queries**
   - **File**: `sql_queries.py`
   - **Description**: Contains SQL queries for creating, dropping, and inserting into tables. Also includes the queries for copying data from S3 to Redshift staging tables.

### ETL Pipeline

```mermaid
graph LR
    S3[User Activity Logs and Song Metadata on S3]
    S3 -->|Load| StagingTables[Staging Tables on Redshift]
    StagingTables -->|Transform and Load| FactTable[Fact Table: songplays]
    StagingTables -->|Transform and Load| DimensionTables[Dimension Tables]
    
    DimensionTables --> Users[users]
    DimensionTables --> Songs[songs]
    DimensionTables --> Artists[artists]
    DimensionTables --> Time[time]
