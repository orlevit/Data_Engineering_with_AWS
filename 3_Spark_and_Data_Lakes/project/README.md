# Spark and Human Balance

## Project Overview

In this project, we use Spark and AWS Glue to process, categorize, and curate sensor data from multiple sources to build a data lakehouse solution for STEDI, a human balance analytics system. This involves creating curated datasets for training machine learning models that accurately detect steps using STEDI's Step Trainer and mobile app sensor data.

## Project Introduction: STEDI Human Balance Analytics

As a data engineer for the STEDI team, your task is to develop a solution to handle data from the STEDI Step Trainer and its companion mobile app. The Step Trainer measures distances using motion sensors, while the mobile app uses accelerometers to track motion. You will process and curate this data to build a machine learning model that detects steps in real-time.

## Project Details

### STEDI Step Trainer

The STEDI Step Trainer helps users practice balance exercises. It collects data via:
- **Motion sensors**: Measures the distance of detected objects.
- **Mobile app accelerometer**: Records motion in the X, Y, and Z directions.

### Data Sources

You will use three JSON data sources:
1. **Customer Data**: Information about customers.
2. **Step Trainer Data**: Data from the Step Trainer sensors.
3. **Accelerometer Data**: Data from the mobile app accelerometers.

### Requirements

1. **Data Landing Zones**:
   - Create S3 directories for `customer_landing`, `step_trainer_landing`, and `accelerometer_landing`.
   - Copy the JSON data to these directories.

2. **Glue Tables for Landing Zones**:
   - Create Glue tables for `customer_landing` and `accelerometer_landing` using SQL scripts.
   - Query these tables using Athena and provide screenshots of the resulting data.

3. **Sanitize and Create Trusted Data**:
   - Write AWS Glue jobs to sanitize customer and accelerometer data for customers who agreed to share their data.
   - Create Glue tables `customer_trusted` and `accelerometer_trusted`.
   - Verify the jobs' success by querying the `customer_trusted` table with Athena and providing a screenshot.

4. **Curated Customer Data**:
   - Handle a serial number defect in the customer data by cross-referencing accelerometer data.
   - Create a Glue table `customers_curated` for customers who have accelerometer data and agreed to share their data.

5. **Trusted Step Trainer Data and Aggregated Machine Learning Data**:
   - Create Glue Studio jobs to populate `step_trainer_trusted` with Step Trainer Records data.
   - Create an aggregated table `machine_learning_curated` with combined Step Trainer and accelerometer readings for customers who agreed to share their data.

## Code Files

- `machine_learning_curated.py`: Glue job script to create the `machine_learning_curated` table.
- `step_trainer_trusted.py`: Glue job script to create the `step_trainer_trusted` table.
- `accelerometer_landing.sql`: SQL script to create the `accelerometer_landing` table.
- `accelerometer_trusted.sql`: SQL script to create the `accelerometer_trusted` table.
- `create_accelerometer_trusted.ipynb`: Jupyter notebook to create the `accelerometer_trusted` Glue job.
- `create_customer_trusted.ipynb`: Jupyter notebook to create the `customer_trusted` Glue job.
- `customer_curated.sql`: SQL script to create the `customer_curated` table.
- `customer_landing.sql`: SQL script to create the `customer_landing` table.
- `customer_trusted.sql`: SQL script to create the `customer_trusted` table.
- `step_trainer_landing.sql`: SQL script to create the `step_trainer_landing` table.

## Setup Instructions

1. **Create S3 Buckets**:
   - Create S3 directories for `customer_landing`, `step_trainer_landing`, and `accelerometer_landing`.
   - Upload the respective JSON data files to these directories.

2. **Create Glue Tables**:
   - Use the provided SQL scripts to create Glue tables for the landing zones.

3. **Run Glue Jobs**:
   - Use the provided Jupyter notebooks and Python scripts to create and run the Glue jobs for trusted and curated data.

4. **Verify and Aggregate Data**:
   - Use Athena to verify the results of the Glue jobs.
   - Use Glue Studio jobs to create aggregated machine learning data tables.

## Screenshots

Provided the following screenshots:
- `customer_trusted.gif`
- `customer_curated.gif`
- `accelerometer_trusted.gif`
- `accelerometer_curated.gif`
- `step_trainer_trusted.gif`
- `step_trainer_curated.gif`
- `machine_learning_curated.gif`
