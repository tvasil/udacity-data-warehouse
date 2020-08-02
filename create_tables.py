import configparser
import logging
import psycopg2
import sys

from sql_queries import create_table_queries, drop_table_queries

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    filename='dwh_udacity.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER.addHandler(logging.StreamHandler(sys.stdout))


def drop_tables(cur, conn):
    """Executes SQL queries from sql_queries to drop tables"""

    for query in drop_table_queries:
        table_name = query.split("DROP TABLE IF EXISTS ")[1]
        LOGGER.info("-------------------------------")
        LOGGER.info(f"Dropping table {table_name}.\n")
        try:
            cur.execute(query)
            conn.commit()
            msg = f"Finished dropping table {table_name}.\n\n"
            LOGGER.info(msg)
        except Exception:
            msg = f"Exception occurred while dropping table {table_name}"
            LOGGER.exception(msg)


def create_tables(cur, conn):
    """Executes SQL queries from sql_queries to create needed tables"""
    for query in create_table_queries:
        table_name = query.split()[5]
        LOGGER.info("-------------------------------")
        LOGGER.info(f"Creating table {table_name}:\n")
        try:
            cur.execute(query)
            conn.commit()
            msg = f"Finished creating table {table_name}.\n\n"
            LOGGER.info(msg)
        except Exception:
            msg = f"Exception occurred while creating table {table_name}"
            LOGGER.exception(msg)


def main():
    """Connects to Redshift cluster and then drops(if they exist) tables
    before creating them, from the sql_queries"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    LOGGER.info("Finished dropping tables.")

    create_tables(cur, conn)
    LOGGER.info("Finished creating tables.")

    conn.close()


if __name__ == "__main__":
    main()