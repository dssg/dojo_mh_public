import re
import yaml
import pandas as pd
from utils.helpers import get_database_connection
from pipeline import time_splitter
from utils.constants import (
    SQL_NUMERICAL_CATEGORICAL_FEATURES,
    SQL_AGGREGATE_FEATURES,
    SQL_CREATE_FEATURES,
    CONFIG_PATH
)


def get_numcat_feature_cols(config, table_name, type):
    """Creates the SELECT script of SQL query for numerical / categorical feature columns.
    Both column types use the same SQL template, hence this combined function.

    Args:
        config (dict): config dictionary
        table_name (str): table name the features are drawn from
        type (str): either categorical or numerical

    Returns:
        list (feature query, type)
    """
    conf = config['features'][table_name]
    knowledge_date = conf['knowledge_date']
    table_prefix = conf['table_prefix']

    # Access the 'numerical' or 'categorical' feature specification
    features = conf['features'][type]

    with open(SQL_NUMERICAL_CATEGORICAL_FEATURES, 'r') as f:
        numcat_query = f.read()

    # Create SELECT script for each column
    feature_cols = []
    for col_prefix, feature_params in features.items():

        arg = feature_params['arg']
        impute_val = feature_params['impute_val']
        filter = feature_params.get('filter', '')

        feature_name = '_'.join([table_prefix, col_prefix])

        query = numcat_query.format(
            arg=arg,
            filter=filter,
            knowledge_date=knowledge_date,
            feature_name=feature_name,
            impute_val=impute_val
        )

        feature_cols.append(query)

    return ', \n'.join(feature_cols)


def get_agg_feature_cols(config, table_name):
    """Creates the SELECT script of SQL query for aggregate feature columns.

    Args:
        config (dict): config dictionary
        table_name (str): table name the features are drawn from

    Returns:
        str: aggregate feature query
    """
    conf = config['features'][table_name]
    knowledge_date = conf['knowledge_date']
    table_prefix = conf['table_prefix']
    impute_agg = conf['impute_agg']

    features = conf['features']
    agg_features = features['agg']

    with open(SQL_AGGREGATE_FEATURES, 'r') as f:
        agg_query = f.read()

    # Create SELECT script for each column
    feature_cols = []
    for col_prefix, feature_params in agg_features.items():

        agg_func = feature_params['agg_func'] # e.g. SUM, AVG
        agg_arg = feature_params['agg_arg']
        intervals = feature_params['intervals'] # e.g. '7 days', '1 month'
        add_filter = feature_params.get('add_filter', '')

        for fn in agg_func:
            for interval in intervals:

                feature_name = '_'.join([
                    table_prefix,
                    col_prefix,
                    fn.lower(),
                    re.findall('\d+', interval)[0] + re.search('[a-zA-Z]', interval)[0]
                ])

                query = agg_query.format(
                    agg_func=fn,
                    agg_arg=agg_arg,
                    interval=interval,
                    add_filter=add_filter,
                    knowledge_date=knowledge_date,
                    feature_name=feature_name,
                    impute_agg=impute_agg
                )

                feature_cols.append(query)

    return ', \n'.join(feature_cols)


def get_feature_cols(config, table_name):
    """Creates the SQL script for the feature columns.
    Separates them into categorical and aggregate / numerical.
    Categorical columns are saved later into a table with _cat as ending.
    Numerical/aggregate columns are saved later into a table with _num as ending.

    Args:
        config (dict): config dictionary
        table_name (str): table_name

    Returns:
        tuple: categorical feature columns, joined numerical and aggregate feature columns
    """
    conf = config['features'][table_name]
    features = conf['features']

    cat_feature_cols = None
    aggnum_feature_cols = []
    types = features.keys()

    if 'categorical' in types:
        cat_feature_cols = get_numcat_feature_cols(config, table_name, 'categorical')

    if 'numerical' in types:
        query = get_numcat_feature_cols(config, table_name, 'numerical')
        aggnum_feature_cols.append(query)

    if 'agg' in types:
        query = get_agg_feature_cols(config, table_name)
        aggnum_feature_cols.append(query)

    # Join aggregate and numerical features
    aggnum_feature_cols = ', \n'.join(aggnum_feature_cols)

    return cat_feature_cols, aggnum_feature_cols


def create_feature_table(db_conn, config, as_of_dates, table_name, template_query):
    """Creates features table drawn from a single table of origin.

    Args:
        db_conn (sqlalchemy.engine.base.Connection): database connection
        config (dict): dict holding information about the features
        as_of_dates (list): list of as of dates
        table_name (str): table name that the features are drawn from
        create_query (str): template SQL to create the features table
    """
    conf = config['features'][table_name]
    knowledge_date = conf['knowledge_date']
    from_arg = conf['from_arg']

    cat_feature_cols, aggnum_feature_cols = get_feature_cols(config, table_name)

    # If this table has a categorical specification, execute the SQL query
    if cat_feature_cols is not None:
        formatted_query = template_query.format(
            as_of_dates=', '.join([f"'{x}'" for x in as_of_dates]),
            feature_cols=cat_feature_cols,
            knowledge_date=knowledge_date,
            from_arg=from_arg,
            feature_table_name=table_name + '_cat'
        )
        db_conn.execute(formatted_query)
        db_conn.execute('COMMIT')

    # If this table has an aggregate or numerical specification, execute the SQL query
    if aggnum_feature_cols is not None:
        formatted_query = template_query.format(
            as_of_dates=', '.join([f"'{x}'" for x in as_of_dates]),
            feature_cols=aggnum_feature_cols,
            knowledge_date=knowledge_date,
            from_arg=from_arg,
            feature_table_name=table_name + '_num'
        )
        db_conn.execute(formatted_query)
        db_conn.execute('COMMIT')


def create_features(db_conn, config, as_of_dates, psql_role):
    """Creates features for all tables in the config file.

    Args:
        db_conn (sqlalchemy.engine.base.Connection): database connection
        config (dict): dict holding information about the features
        as_of_dates (list): list of as of dates
        table_name (str): table name that the features are drawn from
    """
    conf = config['features']
    table_names = conf.keys()

    # Drop and create empty features schema
    query = '''
        set role {psql_role};
        drop schema if exists features cascade;
        create schema features;
    '''.format(psql_role=psql_role)
    db_conn.execute(query)
    db_conn.execute('COMMIT')

    # Create feature tables
    with open(SQL_CREATE_FEATURES, 'r') as f:
        template_query = f.read()
    template_query = template_query.format(psql_role=psql_role)
    for table_name in table_names:
        create_feature_table(db_conn, config, as_of_dates, table_name, template_query)

    # Create indices for feature tables
    feats_table_names_q = "select table_name from information_schema.tables where table_schema = 'features';"
    feats_table_names = pd.read_sql(feats_table_names_q, db_conn)
    for i, table_name in enumerate(feats_table_names['table_name']):
        joid_idx_q = f"""create index idx_joid_t{i} on features.{table_name} (joid);"""
        aod_idx_q = f"""create index idx_aod_t{i} on features.{table_name} (as_of_date);"""
        joid_aod_idx_q = f"""create index idx_joid_aod_t{i} on features.{table_name} (joid, as_of_date);"""
        db_conn.execute(joid_idx_q)
        db_conn.execute(aod_idx_q)
        db_conn.execute(joid_aod_idx_q)
        db_conn.execute('COMMIT')


if __name__ == '__main__':

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    db_conn = get_database_connection()
    folds_spec = time_splitter.get_time_split(config)
    as_of_dates = time_splitter.get_all_dates(folds_spec)

    conf = config['features']
    create_features(db_conn, config, as_of_dates)
