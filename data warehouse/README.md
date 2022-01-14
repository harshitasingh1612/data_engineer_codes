#PROJECT SUMMARY:

This project focuses on analyzing the data of a music Streaming app Sparkify.Sparkify collects the information related to songs and the activities which different users perform over their app. They want to move their data and processes onto the cloud.Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
The goal is to  build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

#TECHNICAL ARCHITECTURE:

There are 2 kinds of datasets residing in S3:
Song data : Each file is in JSON format and contains metadata about a song and the artist of that song.
Log data  : Each file is in JSON format and simulate activity logs from the app based on specified configurations.

##DATABASE:                                              
We have built a Star schema for our analysis.

###Fact :
songplays (records in log data associated with song plays)

###Dimensions :
1.users(list of users in the app)
2.songs(list of songs in the database)
3.artists(list of artists in the database)
4.time(timestamps of records in songplays broken down into specific units like hour,day,week etc..)

##DATA FILES
    
1.create_tables.py : 
This file creates the fact and dimension tables for the star schema in Resdhift.

2.etl.py : 
This ETL file loads data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.  

5.sql_queries.py :
This file contains all sql queries(insert records,select etc.), and is imported into the two files above.

#PROJECT STEPS:

1.Creation of Tables
-Design schemas for fact and dimension tables.
-Write CREATE statements in sql_queries.py to create each table.
-Write the logic in create_tables.py to connect to the database and create these tables
-Write DROP statements in sql_queries.py to drop each table if it exists.
-Run create_tables.py through !python create_tables.py in the console to reset the database and tables.
-Launch a redshift cluster and create an IAM role that has read access to S3.
-Add redshift database and IAM role info to dwh.cfg.
-Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.

2.Build ETL Pipeline
-Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
-Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
-Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
-Delete your redshift cluster when finished.

