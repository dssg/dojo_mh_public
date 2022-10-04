import os
import json
import logging
import numpy as np
import pandas as pd
from joblib import load
from pathlib import Path
from datetime import datetime
from sqlalchemy.sql import text
from sqlalchemy import create_engine
from matplotlib import pyplot as plt
from utils.constants import SQL_CREATE_EXPERIMENTS_PATH, LABEL_MAPPING, MODELS_PATH, LOGS_PATH


# General setup for plots
plt.rc("axes.spines", top=False, right=False)


def to_date(string: str) -> datetime.date:
    """Converts a string of form '2020-01-01' into a date.

    Args:
        string (str): date as string

    Returns:
        date (datetime.date): string as date
    """
    return datetime.strptime(string, '%Y-%m-%d').date()


def get_database_connection():
    """Connects to the database.

    Returns:
        sqlalchemy.engine.base.Connection: Connection to the database
    """
    # Get details from ~/.bashrc
    user=os.getenv('PGUSER')
    password=os.getenv('PGPASSWORD')
    host=os.getenv('PGHOST')
    port=int(os.getenv('PGPORT'))
    database=os.getenv('PGDATABASE')
    
    engine = create_engine(
        'postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database)
    )
    
    return engine.connect()


def resolve_proj_dir():
    """ Return project directory e.g., '~/dojo-mh'.
    Use os.path.join() to join the path.
    """
    proj_dir = Path(os.getcwd()).resolve()
    while proj_dir.name != 'dojo_mh':
        proj_dir = proj_dir.parent
    return proj_dir


def get_all_table_names(db_conn, schema='raw'):
    """Get all table names from a particular schema.

    Args:
        db_conn: sqlalchemy database connection; get using get_database_connection()
        schema (str)
    """
    query = f'''
    select * from information_schema.tables where table_schema = '{schema}';
    '''
    df = pd.read_sql(query, db_conn)
    return df['table_name'].to_list()


def get_column_names(db_conn, table: str, schema: str ='raw') -> list[str]:
    """Get column names for table.

    Args:
        table (str): table name
        schema (str): schema name
    
    Returns Pandas DataFrame
    """
    query = f'''
    select * from information_schema.columns
    where table_schema = '{schema}' and table_name = '{table}'
    '''
    return pd.read_sql(query, db_conn)['column_name']
    

def get_cols_overlap(db_conn, table1, table2, schema='raw'):
    """
    Get columns that overlap between two tables.

    Args:
        db_conn: sqlalchemy database connection; get using get_database_connection()
        table1 (str): table name
        table2 (str): table name
        schema (str): schema name

    Returns Pandas DataFrame 
    """
    query = f'''
    with t1 as (
        select * from information_schema.columns
        where table_schema = '{schema}' and table_name = '{table1}'
    ),
    t2 as (
        select * from information_schema.columns
        where table_schema = '{schema}' and table_name = '{table2}'
    )
    select t1.column_name, t1.table_name, t2.table_name from t1
    inner join t2
    on t1.column_name = t2.column_name;
    '''

    return pd.read_sql(query, db_conn) 


def get_matching_tables(db_conn, table, string_match, schema='raw'):
    """Get tables that match on columns with a particular table.

    Args:
        db_conn (sqlalchemy.engine.base.Connection): database connection
        table (string): table name
        string_match (string): string to match all tables on
        schema (str, optional): schema, defaults to 'raw'

    Returns:
        list: list of data frames giving the meta data on the tables that match
    """
    all_tables = get_all_table_names(db_conn, schema=schema)
    sel = np.array([string_match in table for table in all_tables])

    sel_tables = np.array(all_tables)[sel]

    df = []
    for tab in sel_tables:
        if tab != table:
            df.append(get_cols_overlap(db_conn, tab, table))

    matches = [x for x in df if x.shape[0] > 1]

    return matches
    

def create_experiment_table(db_conn):
    """Create empty experiments table if it does not exist.

    Args:
        db_conn (sqlalchemy.engine.base.Connection): database connection
    """
    with open(SQL_CREATE_EXPERIMENTS_PATH, 'r') as f:
        query = f.read()

    db_conn.execute(query)
    db_conn.execute("COMMIT")


def insert_experiment_table_start(db_conn, config: dict):
    """Inserts data about the experiment into the results.experiment table

    Args:
        db_conn (sqlalchemy.engine.base.Connection): database connection
    """
    # Get current datetime as a string
    now = datetime.now()
    starts = now.strftime("%Y-%m-%d %H:%M:%S")

    # Using SQLAlchemy sanitizes the json blob
    data = {
        "starts": starts, "ends": None,
        "trained_on": config['county'], "config": json.dumps(config),
        "label_group": get_label_group(config)
    }
    statement = text(
        """
        INSERT INTO results.experiments(starts, ends, config, trained_on, label_group) 
        VALUES(:starts, :ends, :config, :trained_on, :label_group) returning experiment_id
        """
    )

    result = db_conn.execute(statement, **data)
    experiment_id = result.fetchone()[0]

    return experiment_id


def insert_experiment_table_end(db_conn, experiment_id: int):
    """Inserts end datetime of the experiment into the results.experiment table

    Args:
        db_conn (sqlalchemy.engine.base.Connection): database connection
        experiment_id (int): experiment id
        ends (string): datetime string
    """
    # Get current datetime as a string
    now = datetime.now()
    ends = now.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
    update results.experiments
    set ends = '{ends}'::timestamp
    where experiment_id = {experiment_id}::int
    """

    db_conn.execute(query)
    db_conn.execute("COMMIT")


def get_label_group(config):
    """Returns a human-readable form of the selected labels

    Args:
        config (dict): config dictionary
    Returns:
        label group (str)
    """
    selected_labels = sorted(config['labels']['selected_labels'])

    for key, val in LABEL_MAPPING.items():
        for labels in val:
            if selected_labels == sorted(labels):
                return key
    
    raise Exception('Label group not defined')


def get_label_tablename(config: dict):
    """Creates the label table name from the config.
    
    Args:
        config: config dictionary

    Returns:
        - a string to use for the label table name
    """

    label_group = get_label_group(config)
    label_group = label_group.replace(' ', '_').replace('-', '_').lower()

    county = config['county']
    label_tablename = '_'.join(['label', label_group, county])

    return label_tablename


def get_label_counts(db_conn, label_names, label_group, distinct_joids=False, county='joco'):
    """Return counts of true labels across the cohort for a particular county.

    Args:
        db_conn (sqlalchemy database connection)
        label_names (list): list of strings giving the label names
        label_group (str): string indicating the label group
        distinct_joids (boolean, False): whether to look at distinct people or overall events
        county (str, 'joco'): string indicating the county

    Returns:
        dataframe of counts of true labels across as of dates
    """
    str_label_names = ','.join(["'" + name + "'" for name in label_names])
    to_count = 'distinct joid' if distinct_joids else '*'

    query = f'''
        select count({to_count}), as_of_date from modeling.split_labels
        where label_name in({str_label_names})
        and label
        and county = '{county}'
        group by as_of_date;
    '''
    
    df = pd.read_sql(query, db_conn)
    df['County'] = 'Johnson' if county == 'joco' else 'Douglas'
    df['Label group'] = label_group
    df = df.rename(columns={'as_of_date': 'As of date', 'count': 'Count'})

    return df

def get_label_from_experiment_table(db_conn, experiment_id) :
    """Returns the list of labels used for a specific experiment id

    Args:
        - db_conn (sqlalchemy database connection)
        - experiment_id: The id for the given experiment

    Returns:
        a list of labels
    """
    query = f'''
    select config -> 'labels' -> 'selected_labels' as label_group
    from results.experiments
    where experiment_id = {str(experiment_id)}
    '''

    label_group = list(pd.read_sql(query, db_conn)['label_group'])[0]
    return label_group


def start_logger_if_necessary(logger_now):
    logger = logging.getLogger("mylogger")
    if len(logger.handlers) == 0:
        logger.setLevel(logging.INFO)
        # Uncomment lines below to also log to stdout
        # sh = logging.StreamHandler()
        # sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
        # logger.addHandler(sh)
        log_path = os.path.join(LOGS_PATH, 'pipeline_log_'
             + str(logger_now).replace(' ', '_') + '.log'
        )
        fh = logging.FileHandler(
            log_path, mode='w'
        )
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
        logger.addHandler(fh)

    return logger


def get_model_set_ids(db_conn, experiment_id):
    """Gets model set ids given an experiment id.

    Args:
        db_conn (sqlalchemy database connection): database connection
        experiment_id (int): experiment id

    Returns:
        numpy array of model set ids
    """
    query = f'''
        select * from results.model_sets
        inner join results.experiments e 
        using(experiment_id)
        where e.experiment_id = {experiment_id};
    '''

    return pd.read_sql(query, db_conn)


def get_model_ids(db_conn, model_set_id):
    """Gets model ids given a model set id

    Args:
        db_conn (sqlalchemy database connection): database connection
        model_set_id (int): model set id
    
    Returns:
        numpy array of model ids
    """
    query = f'''
        select model_id from results.models
        inner join results.model_sets s 
        using(model_set_id)
        where s.model_set_id = {model_set_id};
    '''

    return np.array(pd.read_sql(query, db_conn)['model_id'])


def load_models(model_name, exp_id):
    """Loads all models of a particular name and experiment.

    Args:
        model_name (str): model name, e.g. 'RandomForestClassifier'
        exp_id (int): experiment id

    Returns:
        list of loaded models
    """

    start = '_'.join([model_name, str(exp_id)]) + '_'
    files = os.listdir(MODELS_PATH)
    files_sel = [file for file in files if start in file]
    return [load(os.path.join(MODELS_PATH, file)) for file in files_sel]
