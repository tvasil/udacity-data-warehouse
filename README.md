# Redshift Data Warehouse ETL
This repository is for the Project 3 of the [Udacity Data Engineer Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027), submitted in August 2020. It builds an ETL pipeline, which copies publicly available data from an S3 bucket, creates staging tables in Redshift and finally loads the copies data into a relational Redshift database, demonstrating understanding of distributed cloud warehousing, ETL processes and data quality checks.

## Summary
This project's aim is to create a cloud Data Warehouse for Sparkify, a fictive music-streaming service, so that it can analyze song plays by its users. The requirements of the project include building a relational database, which will allow analyst and other business stakeholders to understand songplays by users and answer questions such as:
- Which songs are users listening to most?
- Which artists are users listening to most?
- Who are our most common listeners, in terms of location, (subscription) level, gender?
- What are the most popular times for streaming music through Sparkify?

The DWH data comes from two sources:
1. Logs of users listening to specific songs on Sparkify (generated data)
2. Database of song / artist information from

## How to run
This ETL process has two steps:
### 1. Run `create_tables.py` in bash:

```bash
python3 create_tables.py
```
This will run the script that drops any tables if they already exist (to run from a clean slate) and then creates the necessary tables. This includes
a) staging tables (into which we will bulk copy data from S3)
b) dimension tables (`users`, `artists`, `songs`, `time`)
c) fact table (`songplays`)

This script parses through the `dwh.cfg` file, which should store:
- information about which S3 bucket to copy from and how
- AWS key and secret to connect to Redshift
- Cluster information of the Redshift cluster we want to connect to through `psycopg2`
- IAM_ROLE ARN infomation, to serve as the credential to S3.

Finally it should be noted that an active Redshift cluster should already be running _before_ you run the above script, otherwise there is no `CLUSTER` or database to connect to.

### 2. Run `etl.py` in bash
```bash
python3 `etl.py`
```
This script is the main ETL process of the project. First, it connects again to the Redshift cluster and does the following:
- Copy data from the Udacity S3 bucket into staging tables in Redshift
- Clean up the data from the staging tables into a star-schema database in Redshift.

## Credentials
As noted above, this script will not run if you don't configure:
1. a `dwh.cfg` file that holds `CLUSTER`, `IAM_ROLE` and `S3` data.
2. an `available` Redshift cluster, whose information is provided in `dwh.cfg`

## Logging
All output of the ETL process will be looged ot the `dwh_udacity.log` file, with custom logging level to `INFO`. This helps troubleshoot future issues, by logging errors and status updates.
