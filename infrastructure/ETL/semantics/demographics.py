"""
Build demographics table
"""

import os
import json
import sqlalchemy
import pandas as pd
import sys

from utils.helpers import get_database_connection
from utils.constants import DEMOGRAPHICS_JSON_PATH, EVENT_DATE_JSON_PATH, SQL_CREATE_EMPTY_DEMOGRAPHICS_PATH, SQL_INSERT_DEMOGRAPHICS_PATH


def create_empty_demographics(db_conn, psql_role):
    """
    Create empty demographics table.
    """

    with open(SQL_CREATE_EMPTY_DEMOGRAPHICS_PATH, 'r') as f:
        query = f.read()

    query.format(psql_role=psql_role)
    db_conn.execute(query)
    db_conn.execute("COMMIT")


def insert_demographics(db_conn):
    """
    Insert values to client_events table.
    """
    with open(DEMOGRAPHICS_JSON_PATH, 'r') as f:
        demographics_dict = json.load(f)

    with open(EVENT_DATE_JSON_PATH, 'r') as f:
        event_date_dict = json.load(f)

    with open(SQL_INSERT_DEMOGRAPHICS_PATH, 'r') as f:
        query = f.read()

    for table_name, vals in demographics_dict.items():

        formatted_query = query.format(
            event_date = event_date_dict[table_name]['event_date'],
            schema = vals["schema"],
            type_demographics_raw = vals["type_demographics_raw"],
            type_demographics = vals["type_demographics"],
            table_name = table_name
        )

        db_conn.execute(formatted_query)
        db_conn.execute("COMMIT")

        print("Demographics built for table: ", table_name)

def get_semi_clean_tables(db_conn):
    '''
    Return a list of the semi clean tables.

    Args:
    db_conn: The sql db connection
    '''
    sql_query = '''
    select distinct(table_name) from information_schema.columns
    where table_schema = 'semi_clean';'''

    semi_clean_tables = pd.read_sql(sql_query, db_conn)['table_name'].to_list()
    return semi_clean_tables

if __name__ == "__main__":

    psql_role = sys.argv[1]
    db_conn = get_database_connection()
    create_empty_demographics(db_conn, psql_role)
    insert_demographics(db_conn)
