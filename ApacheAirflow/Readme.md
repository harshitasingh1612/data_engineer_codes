#PROJECT SUMMARY:

This project focuses on analyzing the data of a music Streaming app Sparkify.Sparkify collects the information related to songs and the activities which different users perform over their app. 
The goal is to  build  high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
There is s data quality step since it plays a big part when analyses are executed on top the data warehouse.So we want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

#TECHNICAL ARCHITECTURE:

There are 2 kinds of datasets residing in S3:
Song data : Each file is in JSON format and contains metadata about a song and the artist of that song.
Log data  : Each file is in JSON format and simulate activity logs from the app based on specified configurations.

##DATA WAREHOUSE:                                              

###Fact :
songplays (records in log data associated with song plays)

###Dimensions :
1.users(list of users in the app)
2.songs(list of songs in the database)
3.artists(list of artists in the database)
4.time(timestamps of records in songplays broken down into specific units like hour,day,week etc..)

##DATA FILES
    
1.dags : 
This folder contains the main data pipeline where the entire operation is done.

2.plugins :
This folder contains the helpers and operators which performs the actual transformation.
	- Helpers:
    	sql_queries.py : This file contains the sql queries hich will be used for loading the data in the Fact and Dimension tables.
        
	Operators:
    	stage_redshift.py : This operator loads the data from S3 files in the staging tables in Redshift cluster.
        load_fact.py : This operator loads the data from staging tables in the Fact table
        load_dimension.py : This operator loads the data from staging tables in the Dimension tables
        data_quality.py : This operator checks the discrepancy in the dimension tables based on the test query.

3.create_tables.sql : This file contains the SQL queries to create teh tables in the Redshift cluster and needs to be run only once.
