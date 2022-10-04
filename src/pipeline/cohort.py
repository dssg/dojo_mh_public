import yaml
import json
from utils.helpers import get_database_connection
from pipeline.time_splitter import get_time_split, get_all_dates
from utils.constants import CONFIG_PATH, SQL_CREATE_EMPTY_COHORT_PATH, SQL_INSERT_COHORT_PATH, TABLE_COUNTY_PATH


def create_empty_cohort(db_conn, psql_role):
    """Create empty cohort table.
    Args:
        db_conn (sqlachemy database connection)
    """
    with open(SQL_CREATE_EMPTY_COHORT_PATH, 'r') as f:
        query = f.read()
    query = query.format(psql_role=psql_role)

    db_conn.execute(query)
    db_conn.execute('COMMIT')


def insert_cohort(db_conn, as_of_dates, config):
    """Create the cohort table for the model, writes it to database.
    The output table has two columns: joid, as_of_date.

    Args:
        db_conn (sqlachemy database connection)
        as_of_dates (list of str): list of cohort reference dates
        config (dict): config dictionary including years_back and included_tables for the cohort
    """
    interval_back =  config['cohort']['interval_back']
    tables_excluded = ['jocojcdheencounter', 'jocodcmexoverdosessuicides', 'jocojcmexoverdosessuicides']
    tables_excluded_sql = '(' + ', '.join(f"'{t}'" for t in tables_excluded) + ')'

    with open(SQL_INSERT_COHORT_PATH, 'r') as f:
        query = f.read()

    with open(TABLE_COUNTY_PATH, 'r') as f:
        table_county_dict = json.load(f)

    doco_tables = ', '.join([f"'{t}'" for t in table_county_dict["doco_tables"]])
    joco_tables = ', '.join([f"'{t}'" for t in table_county_dict["joco_tables"]])

    for as_of_date in as_of_dates:

        formatted_query = query.format(
            as_of_date = as_of_date,
            interval_back = interval_back,
            doco_tables = doco_tables,
            joco_tables = joco_tables,
            tables_excluded = tables_excluded_sql
        )

        db_conn.execute(formatted_query)
        db_conn.execute('COMMIT')


if __name__ == "__main__":

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    as_of_dates = get_all_dates(get_time_split(config))
    as_of_dates= [dt.strftime('%Y-%m-%d') for dt in as_of_dates]

    db_conn = get_database_connection()
    create_empty_cohort(db_conn)
    insert_cohort(db_conn, as_of_dates, config)
