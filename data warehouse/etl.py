import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import boto3


def load_staging_tables(cur, conn):
    """
    - Loads data from S3 into the staging tables on Resdhift
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - Loads data from staging tables into analytic tables on Redshift

    - Uses both the staging tables to process the data
    for the fact and dimenision tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads the dwh.cfg configuration file

    - Connects to the Redshift cluster

    - Executes the load_staging_tables and insert_tables function.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}"
        .format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
