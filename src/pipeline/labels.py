import yaml
from os.path import join
from utils.helpers import get_database_connection, get_label_tablename
from utils.constants import CONFIGS_PATH, SQL_CREATE_LABELS_PATH, SQL_LABELS_PATH
from pipeline import time_splitter


def create_empty_labels(db_conn, config, psql_role):
    """Create empty labels table on the database.

    Inputs:
        db_conn (sqlalchemy.engine.base.Connection): database connection
        config (dict)
    """
    with open(SQL_CREATE_LABELS_PATH, 'r') as f:
        query = f.read()

    label_tablename = get_label_tablename(config)
    formatted_query = query.format(label_tablename=label_tablename, psql_role=psql_role)

    db_conn.execute(formatted_query)
    db_conn.execute("COMMIT")


def insert_labels(db_conn, as_of_dates, config):
    """Insert the labels data on the database.

    Inputs:
        db_conn (sqlalchemy.engine.base.Connection): database connection
        as_of_dates (list of str): dates of reference for cohort,
            e.g. '2019-01-01'
        config (dict): dict which holds the relevant information including
            - months_future: maximum number of months after the as_of_date for which
                the label is set (e.g. commit suicide/overdose within the next 6 months)
            - selected_labels: labels we want to join
            - definitions: definition of the labels, see config.yaml

    Returns (str) label names.
    """
    # Get config information
    label_tablename = get_label_tablename(config)
    county = config['county']
    config = config['labels']
    months_future = str(config['months_future'])
    selected_labels = config['selected_labels']
    county = "'doco', 'joco'" if county == 'both' else "'" + county + "'"

    # Get SQL queries for the selected labels
    label_names = []
    cte_queries = []
    combination_queries = []
    for _, spec in config['definitions'].items():
        label_name = spec['label_name']
        if label_name in selected_labels:
            label_names.append(label_name)
            cte_queries.append(spec['cte_expression'])
            combination_queries.append(spec['combination_query'])

    # Join the SQL queries together
    label_name = ', '.join(label_names)
    cte_query = 'with ' + ', '.join(cte_queries)
    combination_query = ' union '.join(combination_queries)

    with open(SQL_LABELS_PATH, 'r') as f:
        query = f.read()

    # Insert label information to the table for each date
    for as_of_date in as_of_dates:
        cte_query_update = cte_query.format(
            label_name=label_name,
            as_of_date=as_of_date,
            months_future=months_future
        )

        formatted_query = query.format(
            county=county,
            label_tablename=label_tablename,
            label_name=label_name,
            as_of_date=as_of_date,
            cte_query=cte_query_update,
            combination_query=combination_query
        )

        db_conn.execute(formatted_query)
        db_conn.execute("COMMIT")

    return label_names


def create_split_labels(db_conn, as_of_dates, config, psql_role, label_period = 'validation_period'):
    """Create split labels data in the database. Slightly adjusted from insert_labels.

    Inputs:
        db_conn (sqlalchemy.engine.base.Connection): database connection
        selected_labels (list): labels to put into the dataframe
        as_of_dates (list of string dates): dates of reference for cohort, e.g. '2019-01-01'
        config (dict): dict which holds the relevant information including
            - months_future: maximum number of months after the as_of_date for which
                the label is set (e.g. commit suicide/overdose within the next 6 months)
            - selected_labels: labels we want to join
            - definitions: definition of the labels, see config.yaml
        label_period (str) the period over which to aggragate label outcomes.
            - 'validation_period' will look only over the validation period used for the model
            - 'any' will look over all future dates after the as_of_date
    """
    if label_period == 'any' :
           label_tablename = 'split_labels_all_time'
    else :
        label_tablename = 'split_labels'

    # Create empty table
    create_query = f'''
        set role {psql_role};

        drop table if exists modeling.{label_tablename};

        create table modeling.{label_tablename} (
            joid int,
            as_of_date date,
            county varchar,
            label_name varchar,
            label boolean
        );
    '''
    db_conn.execute(create_query)
    db_conn.execute('COMMIT')

    # Get config information
    config = config['labels']
    months_future = str(config['months_future'])

    with open(SQL_LABELS_PATH, 'r') as f:
        query = f.read()

    # Insert split labels data for each label definition and each date
    for _, spec in config['definitions'].items():

        label_name = spec['label_name']
        cte_query = 'with ' + spec['cte_expression']
        combination_query = spec['combination_query']

        for as_of_date in as_of_dates:

            cte_query_update = cte_query.format(
                label_name=label_name,
                as_of_date=as_of_date,
                months_future=months_future
            )

            if label_period == 'any' :
                date_part = f'''< ('{as_of_date}'::date + interval '{months_future} months')'''
                cte_query_update = cte_query_update.replace(date_part, '< current_date')

            formatted_query = query.format(
                label_tablename=label_tablename,
                label_name=label_name,
                as_of_date=as_of_date,
                cte_query=cte_query_update,
                combination_query=combination_query
            )

            db_conn.execute(formatted_query)
            db_conn.execute('COMMIT')


if __name__ == '__main__' :

    db_conn = get_database_connection()
    config_path = join(CONFIGS_PATH, 'config.yaml') # edit with relevant config file
    with open(config_path) as f:
        config = yaml.safe_load(f)

    psql_role = ''  # TODO: Fill in the psql role here
    if not psql_role:
        raise Exception('Please specify psql_role. E.g., take it as an argument.')
    create_empty_labels(db_conn, config)
    folds_spec = time_splitter.get_time_split(config)
    as_of_dates = time_splitter.get_all_dates(folds_spec)
    insert_labels(db_conn, as_of_dates, config)
    create_split_labels(db_conn, as_of_dates, config)
