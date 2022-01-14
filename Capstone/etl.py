import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import etl_tables
import data_transformation
import data_check

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAXRTRMJU3KTC2IMW7'
os.environ['AWS_SECRET_ACCESS_KEY'] = \
    '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'


def create_spark_session():
    return SparkSession.\
            builder.\
            config("spark.jars.packages",
                   "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
            enableHiveSupport().\
            getOrCreate()


def process_immigration_data(spark,
                             input_data,
                             output_data,
                             immigration_file_name,
                             temperature_file_name,
                             country_mapping_file,
                             airport_mapping_file,
                             airport_file_name,
                             usa_demographics_file_name):
    """This function processed immigration data.
    :param spark: spark session
    :param input_data: S3 bucket location
    :param immigration_file_name: immigration dataset
    :param temperature_file_name: global land temperatures dataset
    :param country_mapping_file: i94res file
    :param airport_mapping_file: i94port file
    :param airport_file_name: airport codes dataset
    :param output_data: S3 bucket location
    """
    # read immigration data file
    immigration_file = input_data + immigration_file_name
    df = spark.read.format('com.github.saurfang.sas.spark').\
        load(immigration_file, forceLowercaseNames=True, inferLong=True)

    # clean immigration spark dataframe
    immigration_df = data_transformation.clean_immigration_missing_values(df)
    immigration_df = immigration_df.withColumn(
        "country_code",
        immigration_df["i94res"].cast('integer'))
    immigration_df = immigration_df.drop("i94res")

    # create calendar dimension table
    calendar_df = etl_tables.create_calendar_dimension(spark,
                                                       immigration_df,
                                                       output_data)

    # get global temperatures data
    temperature_df = process_global_land_temperatures(spark,
                                                      input_data,
                                                      temperature_file_name)

    # processes the airport data
    airport_df = process_airport_data(spark,
                                      input_data,
                                      output_data,
                                      airport_file_name,
                                      airport_mapping_file)

    # create country dimension table
    country_df = etl_tables.create_country_dimension(spark,
                                                     immigration_df,
                                                     temperature_df,
                                                     output_data,
                                                     country_mapping_file)
    # create demographics dimension table
    demographics_df = process_demographics_data(spark,
                                                input_data,
                                                output_data,
                                                usa_demographics_file_name)

    # create visa dimension table
    visa_df = etl_tables.create_visa_type_dimension(immigration_df,
                                                    output_data)

    # create immigration fact table
    fact_df = etl_tables.create_i94immigration_fact(
        spark,
        immigration_df,
        output_data)


def process_demographics_data(spark, input_data, output_data, file_name):
    """This function processed immigration data.
    :param spark: spark session
    :param input_data: S3 bucket location
    :param file_name: demographics dataset
    :param output_data: S3 bucket location
    """
    # load demographics data
    demographics_df = spark.read.csv(input_data+file_name,
                                     inferSchema=True,
                                     header=True, sep=';')

    # clean demographics data
    new_demographics_df = data_transformation.\
        clean_demographics_missing_values(demographics_df)

    # create demographic dimension table
    df = etl_tables.create_US_demographics_dimension(new_demographics_df,
                                                     output_data)

    return df


def process_airport_data(spark,
                         input_data,
                         output_data,
                         file_name,
                         airport_mapping_file):
    """This function processed airport data.
    :param spark: spark session
    :param input_data: S3 bucket location
    :param file_name: airport codes dataset
    :param airport_mapping_file: i94port file
    :param output_data: S3 bucket location
    """
    # load demographics data
    airport_df = spark.read.csv(input_data+file_name,
                                inferSchema=True,
                                header=True)

    # clean demographics data
    new_airport_df = data_transformation.\
        clean_airport_missing_values(airport_df)

    # create demographic dimension table
    df = etl_tables.create_airport_dimension(spark,
                                             new_airport_df,
                                             airport_mapping_file,
                                             output_data)

    return df


def process_global_land_temperatures(spark, input_data, file_name):
    """This function processes global land temperatures data.
    :param spark: spark session
    :param input_data: S3 bucket location
    :param file_name: demographics dataset
    """
    # load data
    temp_file = input_data + file_name
    temp_df = spark.read.csv(temp_file, header=True, inferSchema=True)

    # clean the temperature data
    new_temp_df = data_transformation.clean_temperature_missing_values(temp_df)

    return new_temp_df


def main():
    """ - creates the spark session.
        - reads the config file used to pass the AWS credentials
        - defines the input and output location
        - defines the immigration,temperature,airport,demographics
          and country data sources
        - executes the immigration and demogrpahics functions
    """
    spark = create_spark_session()
    input_data = "s3a://mycapstonebucket1/source_files/"
    output_data = "s3a://mycapstonebucket1/parquet_files/"

    immigration_file_name = "i94_apr16_sub.sas7bdat"
    temperature_file_name = "GlobalLandTemperaturesByCity.csv"
    usa_demographics_file_name = "us-cities-demographics.csv"
    airport_file_name = "airport-codes_csv.csv"
    country_codes = input_data + "i94res.csv"
    airport_codes = input_data + "airport_mapping.csv"

    # load the i94res to country mapping data
    country_mapping_file = spark.read.csv(country_codes,
                                          header=True,
                                          inferSchema=True)
    airport_mapping_file = spark.read.csv(airport_codes,
                                          header=True,
                                          inferSchema=True)

    process_immigration_data(spark,
                             input_data,
                             output_data,
                             immigration_file_name,
                             temperature_file_name,
                             country_mapping_file,
                             airport_mapping_file,
                             airport_file_name,
                             usa_demographics_file_name)

    process_demographics_data(spark,
                              input_data,
                              output_data,
                              usa_demographics_file_name)

    data_check.data_quality_check(spark, output_data)
    res = data_check.check_integrity(spark, output_data)
    if res:
        print("Integrity check passed!")
    else:
        print("Integrity check failed!")


if __name__ == "__main__":
    main()
