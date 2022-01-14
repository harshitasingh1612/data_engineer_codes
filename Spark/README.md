#PROJECT SUMMARY:

This project focuses on analyzing the data of a music Streaming app Sparkify.Sparkify collects the information related to songs and the activities which different users perform over their app. They want to move their data warehouse to a data lake.Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
The goal is to  build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

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
    
1.etl.py : 
This ETL file loads data from S3,connects to the Spark and processes it using Spark and loads the output back in the form of parquet files in S3 bucket.

2.dl.cfg :
This file contains the configuration details for connecting to the AWS console using the required credentials.

#PROJECT STEPS:

1.Build ETL Pipeline
-Implement the logic in etl.py to load data from S3 to Spark.
-Implement the logic in etl.py to process data inside Spark.
-Implement the logic to load the data back from Spark to S3 in the form of parquet files.


Simply run the etl.py file to see the desired parquet files(after the processing) in the S3 bucket.