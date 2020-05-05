import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This query will be responsible for loading data from S3 to redshift using COPY.

"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This query will be responsible for inserting data from staging tables into fact and dimension tables.

"""
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the entrance of the program. First, it will establish the connection to redshift. And then it will run funcion of load_staging tables and insert_tables.
"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()