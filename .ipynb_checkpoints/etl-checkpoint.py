import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""This function takes information from staging table copy_table_queries on sql_queries.py file and import do redshift"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""This function runs insert_table_queries from sql_queries.py file and insert information from json files"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

"""Main function runs the configuration on dwh file, open a connection on redshift and run both functions above"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()