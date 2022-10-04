"""
Build client events module.
"""

import os
import json
import sqlalchemy
import pandas as pd
import sys

from utils.helpers import get_database_connection
from utils.constants import CLIENT_EVENTS_JSON_PATH, EVENT_DATE_JSON_PATH, SQL_CREATE_EMPTY_CLIENT_EVENTS_PATH, SQL_INSERT_CLIENT_EVENTS_PATH


def create_empty_client_events(db_conn):
    """
    Create empty client_events table.
    """

    with open(SQL_CREATE_EMPTY_CLIENT_EVENTS_PATH, 'r') as f:
        query = f.read()

    db_conn.execute(query)
    db_conn.execute("COMMIT")


def insert_client_events(db_conn, tables_to_ignore = []):
    """
    Insert values to client_events table.

    Args:
    db_conn: The sql db connection
    tables_to_ignore: a list of tables in semi-clean to skip when adding to semantic.client_events.

    """

    with open(SQL_INSERT_CLIENT_EVENTS_PATH, 'r') as f:
        query = f.read()

    # Add rows from clean schema
    with open(CLIENT_EVENTS_JSON_PATH, 'r') as f:
        client_events_dict = json.load(f)

    with open(EVENT_DATE_JSON_PATH, 'r') as f:
        event_date_dict = json.load(f)

    for table_name, vals in client_events_dict.items():

        if table_name not in tables_to_ignore:

            formatted_query = query.format(
                event_date = event_date_dict[table_name]['event_date'],
                event_type = vals["event_type"],
                table_name = table_name,
                schema = vals["schema"]
            )

            db_conn.execute(formatted_query)
            db_conn.execute("COMMIT")

            print("Client events built for table: ", table_name)

    # Add rows from semi-clean schema
    semi_clean_tables_query = """
        select table_name
        from information_schema.tables
        where table_schema = 'semi_clean';"""

    semi_clean_table_names = pd.read_sql(semi_clean_tables_query, db_conn)["table_name"]

    for table_name in semi_clean_table_names:

        formatted_query = query.format(
            event_date = "event_date",
            event_type = "SEMI-CLEAN",
            table_name = table_name,
            schema = 'semi_clean'
        )

        db_conn.execute(formatted_query)
        db_conn.execute("COMMIT")

        print("Client events built for table: ", table_name)

if __name__ == "__main__":

    psql_role = sys.argv[1]
    db_conn = get_database_connection()
    create_empty_client_events(db_conn, psql_role)
    insert_client_events(db_conn)
