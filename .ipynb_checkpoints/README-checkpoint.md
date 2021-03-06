# Airflow_s3_redshift_dag
---

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.


## Overview 

This project consists of custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.
The project template package contains three major components for the project:

- The dag template has all the imports and task templates in place.
- The operators folder with operator templates
- A helper class for the SQL transformations


The task dependencies generate the following graph
![Fig 1: Dag task dependencies](dag.png)


This project uses four custom operators to stage the data, transform the data, and run checks on data quality.


**Stage Operator**
The stage operator loads any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.


**Fact and Dimension Operators**
Utilizes the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator  takes as input a SQL statement and target database on which to run the query against. 


**Data Quality Operator**
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.


## Project Datasets

The project uses two datasets that reside on s3. 
1. The song dataset ```s3://udacity-dend/song_data``` is a subset of data from the million song dataset. Each file is in JSON format and containsmeta data about a song and the artist of that song. 

2. The log dataset ```s3://udacity-dend/log_data ``` consists of log files generated by an event simulator based on the songs above. 