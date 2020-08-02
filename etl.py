import configparser
import logging
import sys

import psycopg2

from sql_queries import copy_table_queries, insert_table_queries, data_qual

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    filename='dwh_udacity.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER.addHandler(logging.StreamHandler(sys.stdout))


def load_staging_tables(cur, conn):
    """Executes SQL queries from sql_queries to load data from Udacity S3
    bucket"""
    for query in copy_table_queries:
        LOGGER.info("-------------------------------")
        LOGGER.info(f"Copying from S3:\n{query.split()[1]}")
        try:
            cur.execute(query)
            conn.commit()
        except Exception:
            LOGGER.exception("Exception occurred while copying from S3")
        LOGGER.info("Finished copying.\n\n")


def insert_tables(cur, conn):
    """Executes SQL queries from sql_queries that clean up loaded S3 data and
    inserts the values into the final dim and facts tables"""
    for query in insert_table_queries:
        table_name = query.split()[2]
        LOGGER.info("-------------------------------")
        LOGGER.info(f"Starting inserting into {table_name}.")
        try:
            cur.execute(query)
            conn.commit()
        except Exception:
            msg = f"Exception occurred while inserting {table_name} into DWH"
            LOGGER.exception(msg)
        LOGGER.info(f"Finished inserting {table_name}.\n\n")


def basic_quality_checks(cur, conn):
    """Executes some basic queries to the final Redshift database to ensure that
    data exists"""
    cur.execute(data_qual)
    result = cur.fetchall()
    assert len(result) > 0, "Songplays table has no rows! Check what's wrong"


def main():
    """ Connects to Redshift cluster and runs ETL process to
    1. copy data from S3 to staging tables
    2. transform the data in the staging tables to dimension and fact
    tables in a relational Redshift DWH
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()
            )
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    LOGGER.info("Finished loading staging tables.")

    insert_tables(cur, conn)
    LOGGER.info("Finished building tables in DWH.")

    try:
        basic_quality_checks(cur, conn)
    except Exception:
        LOGGER.exception("There is no data in the database.")

    conn.close()
    LOGGER.info("ALL DONE!")


if __name__ == "__main__":
    main()