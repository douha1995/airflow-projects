# Data pipeline with Airflow

This project provide a comprehensive data pipeline solution to extract, tranform and
load daily weather date into AWS s3 for specific country using openweather api.
The pipeline leverages the combination of data tools and AWS services including Apache Airflow,
AWS S3.

## Table of content
- [Overview] (#overview)
- [Architecture] (#Architecture)
- [Infrastructure] (#Inforastructure)
                    
## Overview
The pipeline is designed to 
1. Extract weather data from openweather api.
2. Store the raw data into AWS S3 bucket from Airflow.
3. Transform the weather data using python Pandas library.
4. Load the transformed data into postgres DB.

## Architecture 
                                      