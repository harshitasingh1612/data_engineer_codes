## **PROJECT SUMMARY**

The purpose of this capstone project is to utilize and apply the concepts learnt thrpughout the course.
Here we are working on analyzing the i94 immigration dataset to answer some common questions like the arrival date of the immigrants in US, demographics of the immigrants, visa type distribution of the immigrants, country of residence of the immigrants,the average temperature per city, port of arrival in US(airport cities) etc. 
We extract data from 4 different sources, the I94 immigration dataset of 2016, city temperature data from Kaggle ,US city demographic data from OpenSoft and Airports data from DataHub.
We design 5 dimension tables: Calendar,visa type, US demographics, airport and country and 1 fact table: Immigration. We use Spark for ETL jobs and store the results in parquet files for downstream analysis.

The project follows the follow steps:
## **Step 1**
### *Scope the Project and Gather Data*

We are working with the following datasets in this project.

* I94 Immigration Data:
This data comes from the US National Tourism and Trade Office. In the past all foreign visitors to the U.S. arriving via air or sea were required to complete paper Customs and Border Protection Form I-94 Arrival/Departure Record or Form I-94W Nonimmigrant Visa Waiver Arrival/Departure Record and this dataset comes from this forms.
This dataset forms the core of the data warehouse and the customer repository has a years worth of data for the year 2016 and the dataset is divided by month.For this project the data is in a folder located at ../../data/18-83510-I94-Data-2016/. Each months data is stored in SAS format sas7bdat. Each file has a three-letter abbreviation for the month name.
For this project we are going to work with data for the month of April. 
However, the data extraction and transformation functions have been designed to work with any month's worth of data.

* World Temperature Data:
This dataset came from Kaggle.The Berkeley Earth Surface Temperature Study combines 1.6 billion temperature reports from 16 pre-existing archives. It is nicely packaged and allows for slicing into interesting subsets(for city,country etc.)
Here we are going to use the file '../../data2/GlobalLandTemperaturesByCity.csv' for our project.

* U.S. City Demographic Data:
This data comes from OpenSoft.This dataset contains information about the demographics of all US cities and census-designated places with a population greateror equal to 65,000.This data comes from the US Census Bureau's 2015 American Community Survey.
We are going to use 'us-cities-demographics.csv' file for the project.

* Airport Code Table: 
This is a simple table of airport codes and corresponding cities. The airport codes may refer to either IATA airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems.Some of the columns contain attributes identifying airport locations, other codes (IATA, local if exist) that are relevant to identification of an airport.

## **Step 2**
### *Explore and Assess the Data*
In this step we will be exploring the various data sources in detail like identifying the missing values,duplicates and clean up the sources accordingly.There is a data_transformation.py file which lists out the various tasks performed upon the data sources.

## **Step 3**
### *Define the Data Model*
Conceptual data model: Based on the provided dataset I have built a data model

### *Fact* :
- i94Immigration
### *Dimensions* : 
- Calendar
- Country
- US Demographics
- Visa type
- Airport

The Calendar dimension is built upon the arrdate(arrival date in US) present in the i94 immigration dataset.

The country dimension table is made up of data from the global land temperatures by city and the immigration datasets.
The combination of these two datasets helps to study correlations between global land temperatures and immigration patterns to the US.

The US demographics dimension table comes from the US Demographics dataset and joined with the immigration fact table at US state level.
This dimension would allow analysts to get insights into migration patterns into the US based on demographics and overall population of states.
We can use this data to determine whether populous states have more visitors,can make some key business decisions to further attract tourism in specific states.

The visa type dimension table comes from the immigration datasets and joined to the immigration via the visa_type_key.

The airport dimension is built upon the dataset(airport-codes.csv) and  i94port codes given in the aiport_mapping file derived from the I94-labels-decsription.SAS

The immigration fact table is made up of i94 immigration dataset available to us on monhly basis.
This data comes from the immigration data sets and contains keys that joins to all the dimension tables. 
The data dictionary of the immigration dataset contains detailed information on the data that makes up the fact table.

## The pipeline steps are as follows:

 1. Load the datasets
 2. Clean up the I94 Immigration data to create Spark dataframe for each month
 3. Create calendar dimension table
 4. Create visa_type dimension table
 5. Extract clean global temperatures data
 6. Create country dimension table
 7. Load demographics data
 8. Clean demographics data
 9. Create demographic dimension table
 10. Create airport dimension table
 11. Create visa_type dimension table
 12. Create immigration fact table
 
## **Step 4**
*Run ETL to Model the Data*
 - We have stored all of the data sources in the input S3 bucket at location "s3a://mycapstonebucket1/source_files/"
 - We have written the fact and dimension tables in the output S3 bucket at location "s3a://mycapstonebucket1/tables/"

There are 4 python files to perform the ETL functionality over the datasets.
 1. *data_transformation.py* - This file transforms the original data sources into a form that could be used for creating the fact and dimension tables.
 2. *etl_tables.py* - This file creates the fact and dimension tables from the original data sources and writes them up in parquet file(s) at S3 output location.
 3. *etl .py* - This file imports the data_transformation.py and etl_tables.py files. It extracts the sas data(immigration data source) from the directory and other data sources,transforms it up using the data_transformation.py and finally loads the data in all the fact and dimension tables using the etl_tables.py file.
 4. *data_check.py* - This file checks the data quality of fact and dimension tables.
 
** There is a data dictionary for the original data sources(Source dataset data dictionay.csv)

** There is a data dictionay for the fact and dimensions(Data Model data dictionary.csv)

** We have created an EMR cluster for efficient data processing.

## **Step 5**
### Complete Project Write Up

* Clearly state the rationale for the choice of tools and technologies for the project.

1. Propose how often the data should be updated and why.

        Monthly basis as i94-immigration data is refreshed on monthly basis.

2. Write a description of how you would approach the problem differently under the following scenarios:

    * The data was increased by 100x.

            Spark can handle such amount of data provided.we have used EMR cluster for data processing.
 
    * The data populates a dashboard that must be updated on a daily basis by 7am every day.

            Airflow can be used to update the dashboard on a daily basis
 
    * The database needed to be accessed by 100+ people.

            We can move the database to Redshift.

## **Note**
I have tried to read the config.cfg file to connect to EMR and S3 but somehow it is failing everytime. So as per the recommendation of my [ticket](https://knowledge.udacity.com/questions/694160) I am hard-coding my credentials in the etl .py python file.

 * Run the EMR cluster with the following command:
spark-submit --deploy-mode client --packages saurfang:spark-sas7bdat:1.1.4-s_2.10 --jars s3://mycapstonebucket1/jar/spark-sas7bdat-1.1.4-s_2.10.jar,s3://mycapstonebucket1/jar/spark-core_2.11-1.5.2.logging.jar --repositories https://repos.spark-packages.org/ --conf spark.unsafe.sorter.spill.read.ahead.enabled=false --conf spark.shuffle.service.enabled=true --conf fs.s3a.fast.upload=true --conf fs.s3a.fast.upload.buffer=bytebuffer --conf spark.local.dir=/mnt1/ --py-files s3://mycapstonebucket1/data_pipeline_files/etl_tables.py,s3://mycapstonebucket1/data_pipeline_files/data_transformation.py,s3://mycapstonebucket1/data_pipeline_files/data_check.py,s3://mycapstonebucket1/source_files/us-cities-demographics.csv,s3://mycapstonebucket1/source_files/i94_apr16_sub.sas7bdat,s3://mycapstonebucket1/source_files/GlobalLandTemperaturesByCity.csv,s3://mycapstonebucket1/source_files/i94res.csv,s3://mycapstonebucket1/source_files/airport-codes_csv.csv,s3://mycapstonebucket1/source_files/airport_mapping.csv s3://mycapstonebucket1/data_pipeline_files/etl.py