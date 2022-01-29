# Spark ETL on databricks based on AWS

This project will use AWS S3, databricks cluster (AWS EC2), databricks.

Raw data is stored in datalake (AWS S3 bucket). In order to do ETL process through spark on databricks, we need to mount the corresponding s3 bucket on DBFS. 

__curated_transform.ipynb__ shows the full process, including: mount bucket, process transformation, write the final dataset into curated zone in datalake for next step (ML). 

We can add it as a job and use aws lambda to trigger databricks job to implement auto ETL process when datalake haing new data in the future.

___
___

Author: Yi Ding
Contact: dydifferent@gmail.com