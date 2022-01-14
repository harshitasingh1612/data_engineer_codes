import configparser
from datetime import datetime
import os
#import pandas as pd
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql.functions import (year, month, dayofmonth,
                                   hour, weekofyear, date_format,
                                   dayofweek,
                                   monotonically_increasing_id)
from pyspark.sql.functions import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Reads song files from the S3 bucket.

    - Processes data in the Spark

    - Creates songs and artists dimension tables.

    - Writes the tables back in the parquet files inside S3 bucket
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/*/*/*.json")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("songs_temp_table")
    songs_table = df.select("song_id", "title",
                            "artist_id", "year", "duration")
    songs_table = songs_table.dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_parquet = songs_table.write.mode('overwrite')\
        .partitionBy("year", "artist_id")\
        .parquet(output_data + "/dimSongs")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location",
                              "artist_latitude", "artist_longitude")\
                      .withColumnRenamed('artist_name', 'name')\
                      .withColumnRenamed('artist_location', 'location')\
                      .withColumnRenamed('artist_latitude', 'latitude')\
                      .withColumnRenamed('artist_longitude', 'longitude')

    artists_table = artists_table.dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_parquet = artists_table.write.mode('overwrite')\
        .parquet(output_data + "/dimArtists")


def process_log_data(spark, input_data, output_data):
    """
    - Reads log files from the S3 bucket.

    - Processes data in the Spark

    - Creates users and time dimension tables.

    - Joins the log and song datasets to create the songplays fact table.

    - Writes the tables back in the parquet files inside S3 bucket
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/2018/*/*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    df.createOrReplaceTempView("users_log_table")
    users_table = df.select("userId", "firstName",
                            "lastName", "gender", "level")
    users_table = users_table.dropDuplicates(['userId'])

    # write users table to parquet files
    users_parquet = users_table.write.mode('overwrite')\
        .parquet(output_data + "/dimUsers")

    # create timestamp column from original timestamp column
    @udf(types.DateType())
    def convert_to_datetime(unix_ts):
        return datetime.fromtimestamp(int(unix_ts) / 1000.0)

    @udf(types.TimestampType())
    def get_timestamp(unix_ts):
        return convert_to_datetime(unix_ts)

    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    df = df.withColumn("datetime", convert_to_datetime(df.ts))

    # extract columns to create time table
    df = df.withColumn("start_time", df.ts)
    df = df.withColumn("hour", hour('datetime'))
    df = df.withColumn("day", dayofmonth('datetime'))
    df = df.withColumn("month", month('datetime'))
    df = df.withColumn("years", year('datetime'))
    df = df.withColumn("week", weekofyear('datetime'))
    df = df.withColumn("weekday", dayofweek('datetime'))
    df.createOrReplaceTempView("time_log_table")
    time_table = df.select("start_time", "hour",
                           "day", "month", "years", "week", "weekday")
    time_table = time_table.dropDuplicates(['start_time'])

    # write time table to parquet files partitioned by year and month
    time_table = time_table.write\
        .mode('overwrite')\
        .partitionBy("years", "month")\
        .parquet(output_data + "/dimTime")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/A/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song & log datasets to create songplays table
    songplays_df = df.join(
                           song_df,
                           (df.artist == song_df.artist_name) &
                           (df.song == song_df.title) &
                           (df.length == song_df.duration),
                           'inner')
    songplays_table = songplays_df.select(
        'start_time',
        col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        col('sessionId').alias('session_id'),
        'location',
        col('userAgent').alias('user_agent'),
        col('years').alias('year'),
        'month')\
        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.mode('overwrite')\
        .partitionBy("year", "month")\
        .parquet(output_data + "/factSongplays")


def main():
    """
    - Executes the song_data and log_data process functions.

    - Defines the input and output S3 cuket location.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacitydatalakebucket/OutputParquet"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
