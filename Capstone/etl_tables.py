import datetime as dt
from pyspark.sql.functions import col, udf, dayofmonth, dayofweek
from pyspark.sql.functions import month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id, upper
from pyspark.sql.functions import trim, regexp_replace
from pyspark.sql import types
from pyspark.sql.types import *


def create_i94immigration_fact(
                               spark, df, output_data):
    """This function creates immigration fact from the immigration
       and global land temperatures data.
        :param spark: spark session
        :param df: spark dataframe of immigration events
        :param output_data: path to write dimension dataframe to
        :return: spark dataframe representing calendar dimension
    """
    dim_airport = spark.read.parquet(output_data + "dimtblAirport/")
    dim_visatype = spark.read.parquet(output_data + "dimtblVisatype/")

    # rename columns
    df = df.withColumnRenamed('cicid', 'record_id')\
        .withColumnRenamed('i94addr', 'state_code')\
        .withColumnRenamed('i94port', 'iata_code')

    # join airport and immigration dfs
    df = df.join(dim_airport,
                 df["iata_code"] == dim_airport["iata_code"],
                 "leftouter")\
           .select(df["*"])

    # join visa and immigration dfs
    df = df.join(dim_visatype,
                 df["visatype"] == dim_visatype["visatype"],
                 "leftouter")\
           .select(df["*"], dim_visatype["visa_type_key"])

    # convert date in datetime format
    @udf(types.DateType())
    def convert_date(x):
        mDt = dt.datetime(1960, 1, 1)
        dlt = mDt + dt.timedelta(days=x)
        return dlt

    df = df.withColumn("arr_date", convert_date(df.arrdate))
 c.provider_id
    df = df.drop("arrdate")\
           .withColumnRenamed("arr_date", 'arrdate')

    # write the immigration fact to parquet file
    df.write.mode("overwrite")\
        .parquet(output_data + "facttbli94Immigration")

    return df


def create_US_demographics_dimension(df, output_data):
    """This function creates a us demographics dimension table from
       the us cities demographics data.
    :param df: spark dataframe of us demographics survey data
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing demographics dimension
    """

    demographics_df = df.withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('Average Household Size', 'average_household_size')\
        .withColumnRenamed('State Code', 'state_code')

    # lets add an id column
    demographics_df = demographics_df.withColumn(
                                        'id',
                                        monotonically_increasing_id())

    # write the US demograsphics dimension to parquet file
    demographics_df.write.mode("overwrite")\
                   .option("path",
                           output_data + "dimtblUSdemographics")\
                   .format('parquet')\
                   .saveAsTable("dimtblUSdemographics")

    return demographics_df


def create_visa_type_dimension(df, output_data):
    """This function creates a visa type dimension from the immigration data.
    :param df: spark dataframe of immigration events
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing calendar dimension
    """

    # create visatype df from visatype column
    visatype_df = df.select('visatype').distinct()

    # add an id column
    visatype_df = visatype_df.withColumn(
                                'visa_type_key',
                                monotonically_increasing_id())

    # write the visa type dimension to parquet file
    visatype_df.write\
               .mode("overwrite")\
               .option("path",
                       output_data + "dimtblVisatype")\
               .format('parquet')\
               .saveAsTable("dimtblVisatype")

    return visatype_df


def aggregate_temperature(df):
    """This function creates an aggregated dataframe of global
       land temperatures based on the country
    :param df: spark dataframe of global land temperatures
    :return: spark dataframe representing aggregated dataframe
     of global land temperatures
    """
    temp_df = df.withColumn('country_name', upper(col('Country')))
    temp_df = temp_df.select(['country_name', 'AverageTemperature'])\
        .groupby('country_name').avg()
    new_df = temp_df.withColumnRenamed(
        'avg(AverageTemperature)', 'average_temperature')

    return new_df


def create_country_dimension(spark, df, temp_df,
                             output_data, country_mapping_file):
    """This function creates a country dimension based on
        aggregated temperature dataframe and country mapping file
    :param spark: spark session
    :param df: spark dataframe of immigration events
    :param temp_df: aggrgated temperatures dataframe
    :param output_data: path to write dimension dataframe to
    :param country_mapping_file: i94res codes file
    :return: spark dataframe representing aggregated dataframe
     of global land temperatures
    """
    # aggregated temperature df
    agg_temp_df = aggregate_temperature(temp_df)

    # extract the country names from the country mapping file
    # based on the country codes
    country_mapping_file = \
        country_mapping_file.withColumn(
            "country_code_int",
            country_mapping_file["country_code"].cast('integer'))
    country_mapping_file = country_mapping_file\
        .drop("country_code")\
        .withColumnRenamed("country_code_int", "country_code")

    country_dim_df = df.join(country_mapping_file,
                             "country_code",
                             "inner")\
                       .distinct()\
                       .select(df["*"],
                               country_mapping_file["country_name"])
    country_dim_df = country_dim_df.withColumn(
                                        'country',
                                        trim(col('country_name')))
    country_dim_df = country_dim_df.withColumn(
                                        'country_replace',
                                        regexp_replace(
                                            col('country'),
                                            "[']",
                                            ""))
    country_dim_df = country_dim_df.drop('country_name', 'country')\
                                   .withColumnRenamed(
                                       'country_replace',
                                       'country_name')

    # extract the average temperature from the aggregated temperature df
    # based on the country names
    country_dim_df = \
        country_dim_df.join(agg_temp_df, "country_name", "inner")\
                      .select(
                          country_dim_df["*"],
                          agg_temp_df["average_temperature"])

    # write the country dimension to a parquet file
    country_dim_df.write.mode("overwrite")\
                  .option("path", output_data + "dimtblCountry")\
                  .format('parquet')\
                  .saveAsTable("dimtblCountry")

    return country_dim_df


def create_calendar_dimension(spark, df, output_data):
    """This function creates an immigration calendar based on arrival date
    :param df: spark dataframe of immigration events
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing calendar dimension
    """

    # create a udf to convert arrival date in SAS format to datetime object
    @udf(types.DateType())
    def convert_date(x):
        mDt = dt.datetime(1960, 1, 1)
        dlt = mDt + dt.timedelta(days=x)
        return dlt

    # create initial calendar df from arrdate column
    calendar_dim_df = df.select(['arrdate'])\
                        .withColumn(
                            "arrdate",
                            convert_date(df.arrdate))\
                        .distinct()

    # expand df by adding other calendar columns
    calendar_dim_df = calendar_dim_df.withColumn(
                                        'arrival_day',
                                        dayofmonth('arrdate'))
    calendar_dim_df = calendar_dim_df.withColumn(
                                        'arrival_week',
                                        weekofyear('arrdate'))
    calendar_dim_df = calendar_dim_df.withColumn(
                                        'arrival_month',
                                        month('arrdate'))
    calendar_dim_df = calendar_dim_df.withColumn(
                                        'arrival_year',
                                        year('arrdate'))
    calendar_dim_df = calendar_dim_df.withColumn(
                                        'arrival_weekday',
                                        dayofweek('arrdate'))

    # create an id field in calendar df
    calendar_dim_df = calendar_dim_df.withColumn(
                                        'id',
                                        monotonically_increasing_id())

    # write the calendar dimension to parquet file
    calendar_dim_df.write\
                   .partitionBy(
                        "arrival_year",
                        "arrival_month",
                        "arrival_week",
                        "arrival_day")\
                   .mode("overwrite")\
                   .option("path", output_data + "dimtblCalendar")\
                   .format('parquet')\
                   .saveAsTable("dimtblCalendar")

    return calendar_dim_df


def create_airport_dimension(spark, df, airport_mapping_file, output_data):
    """This function creates an airport dimension based on the airport codes
    and airport mapping file
    :param df: spark dataframe of airport data
    :param airport_mapping_file: i94port codes file
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing airport dimension
    """
    # extract the airport cities based on the iata codes(airport codes)
    # from the airport mapping file
    airport_mapping_file = airport_mapping_file\
        .withColumnRenamed(
            'airport_code',
            'iata_code')
    airport_join_dim_df = df.join(
                                airport_mapping_file,
                                "iata_code",
                                "inner")\
                            .distinct()\
                            .select(
                                df["*"],
                                airport_mapping_file["airport_city"])

    airport_dim_df = airport_join_dim_df\
        .withColumnRenamed('ident', 'id')\
        .withColumnRenamed('type', 'airport_type')\
        .withColumnRenamed('name', 'airport_name')\
        .withColumnRenamed('elevation_ft', 'elevation_in_ft')

    # write the airport dimension to parquet file
    airport_dim_df.write.mode("overwrite")\
                  .option("path", output_data + "dimtblAirport")\
                  .format('parquet')\
                  .saveAsTable("dimtblAirport")

    return airport_dim_df
