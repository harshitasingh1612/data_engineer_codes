# Do all imports and installs here
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.types import *
from pyspark.sql.functions import isnan, when, count, col


def visualize_missing_values(df):
    """This function visually represents the missing values
       of each dataframe for the cleanup process
    :param df: spark dataframe
    """
    # create a dataframe with missing values \
    # count per column
    nulls_df = df.select([count(when(isnan(c) | col(c).isNull(),
                         c)).alias(c) for c in df.columns]).toPandas()

    # convert dataframe from wide format to long format
    nulls_df = pd.melt(nulls_df, var_name='cols', value_name='values')

    # count total records in df
    total = df.count()

    # now lets add % missing values column
    nulls_df['% missing values'] = 100*nulls_df['values']/total

    plt.rcdefaults()
    plt.figure(figsize=(10, 5))
    ax = sns.barplot(x="cols", y="% missing values", data=nulls_df)
    ax.set_ylim(0, 100)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    plt.show()


def clean_immigration_missing_values(df):
    """This function cleans up the missing values in the columns
       and removes the duplicates in the immigration dataframe
       for the cleanup process
    :param df: spark dataframe of immigration events
    :return df: transformed immigration events dataframe
    """
    # total records in the dataframe
    total_records = df.count()
    print('Total records in the original immigration dataframe: {:,}'.
          format(total_records))

    # drop rows with missing values in the columns listed
    cols = ["occup", "entdepu", "insnum"]
    new_df = df.drop(*cols)
    total_recs_after_dropping_nas = new_df.count()
    print('Total records after dropping rows with missing values: {:,}'.
          format(total_records-total_recs_after_dropping_nas))

    # drop duplicate rows
    new_df.select("visapost", "cicid").dropDuplicates()
    print('Rows dropped after accounting for duplicates: {:,}'.
          format(total_recs_after_dropping_nas-new_df.count()))
    return new_df


def clean_temperature_missing_values(df):
    """This function cleans up the missing values in the columns
       and removes the duplicates in the temperatures dataframe
       for the cleanup process
    :param df: spark dataframe of global land temperautres
    :return df: transformed temperatures dataframe
    """
    # total records in the dataframe
    total_records = df.count()
    print('Total records in the original temperature dataframe: {:,}'.
          format(total_records))

    # drop rows with missing average temperature
    new_df = df.dropna(subset=['AverageTemperature'])
    total_recs_after_dropping_nas = new_df.count()
    print('Total records after dropping rows with missing values: {:,}'.
          format(total_records-total_recs_after_dropping_nas))

    # drop duplicate rows
    new_temperature_df = new_df.drop_duplicates(subset=['dt', 'City',
                                                        'Country'])
    print('Rows dropped after accounting for duplicates: {:,}'.
          format(total_recs_after_dropping_nas-new_temperature_df.count()))
    return new_temperature_df


def clean_demographics_missing_values(df):
    """This function cleans up the missing values in the columns
       and removes the duplicates in the US demographics dataframe
       for the cleanup process
    :param df: spark dataframe of US demographics
    :return df: transformed US demographics dataframe
    """
    # total records in the dataframe
    total_records = df.count()
    print('Total records in the original demographics dataframe : {:,}'.
          format(total_records))

    # drop rows with missing values in the listed columns
    cols = ['Number of Veterans', 'Foreign-born',
            'Average Household Size', 'Male Population',
            'Female Population']
    new_df = df.drop(*cols)
    missing_values_dropped = total_records - new_df.count()
    print('Total records after dropping rows with missing values: {:,}'.
          format(missing_values_dropped))

    # drop duplicate rows
    new_demographics_df = new_df.drop_duplicates(subset=['City', 'State',
                                                         'Race', 'State Code'])
    duplicate_rows_removed = new_df.count() - new_demographics_df.count()
    print('Rows dropped after accounting for duplicates : {:,}'.
          format(duplicate_rows_removed))
    return new_demographics_df


def clean_airport_missing_values(df):
    """This function cleans up the missing values in the columns
       and removes the duplicates in the airport dataframe
       for the cleanup process
    :param df: spark dataframe of airport
    :return df: transformed airport dataframe
    """
    # total records in the dataframe
    total_records = df.count()
    print('Total records in the original airport dataframe : {:,}'.
          format(total_records))

    # drop rows with missing values \
    # in the iata_code
    new_df = df.na.drop(subset=["iata_code"])
    total_recs_after_dropping_nas = new_df.count()
    print('Total records after dropping rows with missing values: {:,}'.
          format(total_records-total_recs_after_dropping_nas))

    return new_df
