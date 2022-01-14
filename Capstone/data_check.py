import boto3
from pyspark.sql.functions import col
from pyspark.sql.types import *


def get_top_layer_folders_under_prefix(bucket_name, prefix):
    """
    Extract the files under a bucket
    :param bucket_name: S3 bucket name
    :param prefix: prefix name
    :return: folder names.
    """
    s3 = boto3.resource(
        's3',
        region_name="us-west-2",
        aws_access_key_id='xxx',
        aws_secret_access_key='xxxx'
    )

    bucket = s3.Bucket(bucket_name)
    tableSet = set()

    for obj in bucket.objects.filter(Prefix=prefix):
        tableSet.add(obj.key.split("/")[1])

    return tableSet


def data_quality_check(spark, output_data):
    """
    Checks the number of records in fact and dimension tables
    :param spark: spark session
    :param output_data: S3 bucket location
    """
    destination_tables = get_top_layer_folders_under_prefix(
        bucket_name="mycapstonebucket1",
        prefix="parquet_files")
    for table in destination_tables:
        data_quality_check_per_table(spark, output_data, table)

            
def data_quality_check_per_table(spark, output_data, table_name):
    """
    Checks the number of records in fact and dimension tables
    :param spark: spark session
    :param output_data: S3 bucket location
    """
    df = spark.read.parquet(output_data + table_name)
    total_records = df.count()
    if(total_records == 0):
        print(f"Data quality check failed for {table_name} with zero records!")
    else:
        print(f"Data quality check passed.")
        print(f"Total number of records in the {table_name} are {total_records}")


def check_integrity(spark, output_data):
    """
    Check the integrity of the model.
    Checks if all the facts columns
    joined with the dimensions has correct values
    :param fact: fact table
    :param dim_airports: airports dimension
    :param dim_visa: visa dimension
    :param dim_mode: mode dimension
    :return: true or false if integrity is correct.
    """
    fact = spark.read.parquet(output_data + "facttbli94Immigration/")
    dim_airport = spark.read.parquet(output_data + "dimtblAirport/")
    dim_visatype = spark.read.parquet(output_data + "dimtblVisatype/")
    dim_calendar = spark.read.parquet(output_data + "dimtblCalendar/")

    integrity_airports = dim_airport.select(col("iata_code")).distinct() \
                                    .join(fact,
                                          fact["iata_code"] ==
                                          dim_airport["iata_code"],
                                          "left_anti") \
                                    .count() == 0

    integrity_visa = dim_visatype.select(col("visa_type_key"))\
                                 .distinct()\
                                 .join(fact,
                                       fact["visa_type_key"] ==
                                       dim_visatype["visa_type_key"],
                                       "left_anti")\
                                 .count() == 0

    integrity_calendar = dim_calendar\
        .select(col("arrdate"))\
        .distinct()\
        .join(fact,
              fact["arrdate"] == dim_calendar["arrdate"],
              "left_anti") \
        .count() == 0

    return integrity_airports & integrity_visa & integrity_calendar
