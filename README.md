# Project 4: Data Lake

## Project Description

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Database Design

This database constitutes a recollection of user activity while listening to music in Sparkify, this could enable to gain insights on trends, personal preferences, and even possible recommendations. This data could be used to explore new features, such as song recommendations, in the startup and ultimately improve the user experience.

The new database schema was designed taking into mind the raw data, the tables respect the original format of the data to make the ETL process easier. The fact table is `songplays`, which contains the log for the user activity in the Sparkify app, the table contains the IDs to the corresponding dimension tables which are: `users`, `songs`, `artists` and `time`.

## ETL Pipeline

 In terms of the ETL pipeline, the process is quite simple, as we read the files containing the raw data stored in an input S3 bucket, then we apply transformation to make the data assimilate into the star schema, finally we stored the transformed tables into parquet files in an output S3 bucket.  

## Project Files

    .
    ├──etl.py: Script in charge of running the ETL pipeline, reading the raw data from S3, transforming it and storing it in a different buckey.
    └──dl.cfg: File that contains the AWS credentials 
    
## How to run the Project

Add a `dl.cfg` file to stored your AWS credentials, then run the `etl.py` script to start the ETL process.
