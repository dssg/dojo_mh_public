import datetime
import itertools
import numpy as np
import os
import pandas as pd
import pickle as p
import zlib
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from time import time
from utils.helpers import get_database_connection, get_column_names
from pipeline.time_splitter import get_time_split, get_train_and_val_dates
from utils.constants import (
    MASTER_MATRIX_DIR, MASTER_MATRIX_PATH,
    SMALL_MATRICES_DIR
)

print('master_df path: ', MASTER_MATRIX_PATH)
print('master df exists: ', os.path.exists(MASTER_MATRIX_PATH))


def delete_matrices_from_disk():
    """Delete the master_df and small matrices from disk."""
    # Remove master_df
    os.remove(MASTER_MATRIX_PATH)
    # remove small mats
    for filename in os.listdir(SMALL_MATRICES_DIR):
        path = os.path.join(SMALL_MATRICES_DIR, filename)
        os.remove(path)


def get_feature_table_names(db_conn, features_schema: str) -> list[str]:
    """ Returns names of feature tables.
    """
    feat_tables_q = f"""
    select * from information_schema.tables f
    where table_schema = '{features_schema}'
    """
    df = pd.read_sql(feat_tables_q, db_conn)
    return list(df['table_name'].values)


def get_cat_and_num_table_names(table_names: list[str]) -> tuple[list[str], list[str]]:
    """ Takes the feature table names and returns the categorical and numerical
    tables, in that order.
    """
    cat_tables, num_tables = [], []  # categorical and numerical feature table names
    for tn in table_names:
        if tn.endswith('_cat'):
            cat_tables.append(tn)
        elif tn.endswith('_num'):
            num_tables.append(tn)
        else:
            raise Exception(f'{tn} does not end in "_cat" or _"num".')
    return cat_tables, num_tables


def standardize_data(X_train: pd.DataFrame, X_val: pd.DataFrame, dtype=np.float32):
    """Scale train and validation features to be standard normal variables
    (mean of 0 and standard deviation of 1).
    """
    scaler = StandardScaler()
    col_names = X_train.columns
    train_idx = X_train.index
    X_train = pd.DataFrame(scaler.fit_transform(X_train), columns=col_names, index=train_idx, dtype=dtype)
    val_idx = X_val.index
    X_val = pd.DataFrame(scaler.transform(X_val), columns=col_names, index=val_idx, dtype=dtype)
    return X_train, X_val


def get_features_matrix(db_conn, feats_schema: str, tables: list[str], mod_schema: str, all_dates_str: str):
    """Get features dataframe for all dates in all_dates_str.

    Args:
    ----
    feats_schema: features' schema name
    tables: names of tables to get features from
    mod_schema: modeling schema name
    all_dates_str: sql-compatible array of dates, as a string
    """
    join_q = ' '.join([f"""
        join {feats_schema}.{table} t{i}
        using (joid, as_of_date)""" for i, table in enumerate(tables)])
    query = f"""
        select *
        from (select joid, as_of_date from {mod_schema}.cohort) c {join_q}
        where as_of_date in {all_dates_str};
        """

    return pd.read_sql(query, db_conn, index_col=['joid', 'as_of_date'])


def get_one_hot_encoder():
    """ Use our desired one_hot_encoder settings.
    """
    return OneHotEncoder(drop=None, handle_unknown='ignore', sparse=False)


def get_all_dates_master(train_dates: list[list], validate_dates: list) -> tuple[list, list, list]:
    """Unravel dates. Easy way of unpacking time splitter output into all the
    desired training and validation dates.
    """
    all_train_dates = set(itertools.chain.from_iterable(train_dates))
    all_validate_dates = set(validate_dates)
    all_dates = all_train_dates | all_validate_dates
    return sorted(list(all_train_dates)), sorted(list(all_validate_dates)), sorted(list(all_dates))


def make_str_array(iterable) -> str:
    """Get string arrays; useful for templated sql queries.
    """
    return '(' + ', '.join([f"'{item}'" for item in iterable]) + ')'


def get_master_df(db_conn, train_dates: list[list[datetime.date]], validate_dates: list[datetime.date]) -> dict:
    """ Writes (if necessary) and returns a dataframe's with all joid and
    as_of_dates that will be required to create the smaller training and
    validation matrices / dataframes.

    Args:
    ----
    db_conn: database connection
    train_dates: training dates to include in master df
    validate_dates: validate dates to include in master df

    Returns a dictionary with keys master_df, num_columns, and cat_columns.
    """
    for tds, validate_date in zip(train_dates, validate_dates):
        assert max(tds) < validate_date

    # If master_df already present, load it
    if os.path.exists(MASTER_MATRIX_PATH):
      print('loading master_df')
      with open(MASTER_MATRIX_PATH, 'rb') as f:
          # Loads a dict, with keys 'master_df', 'num_columns', and 'cat_columns'
          matrices_dict = p.load(f)
      return matrices_dict

    # Master df does not exist already, compute it, store it and return it
    print('computing master_df')
    # TODO: Streamline loading settings; remove hardcoded strings (eventually)
    feats_schema = 'features'
    mod_schema = 'modeling'
    _, _, all_dates = get_all_dates_master(train_dates, validate_dates)
    all_dates_str = make_str_array(all_dates)

    # Get feature table names
    table_names = get_feature_table_names(db_conn, feats_schema)
    cat_tables, num_tables = get_cat_and_num_table_names(table_names) # categorical and numerical feature table names

    # Load dataframes for categorical and numerical features
    num_df = get_features_matrix(db_conn, feats_schema, num_tables, mod_schema, all_dates_str)
    # TODO: Handle this imputation check somewhere
    # assert(sum(num_df.isna().any()) < 2)  # At most 1 numerical col with nulls (dem_age). If there are more should remake the features and  labels tables

    cat_df = get_features_matrix(db_conn, feats_schema, cat_tables, mod_schema, all_dates_str)

    # Ensure there are no repeated feature names
    num_columns = list(num_df.columns)
    cat_columns = list(cat_df.columns)
    if len(set(num_columns + cat_columns)) != len(num_columns + cat_columns):
        repeated_cols = set(num_columns) & set(cat_columns)
        raise Exception(f'{repeated_cols} are both numerical and categorical feature(s).')

    # Features dataframe
    feats_df = cat_df.join(num_df)

    to_write = {
        'master_df': feats_df,
        'num_columns': num_columns,
        'cat_columns': cat_columns,
    }

    # Make master df directory if it does not exist
    if not os.path.exists(MASTER_MATRIX_DIR):
        os.mkdirs(MASTER_MATRIX_DIR)

    with open(MASTER_MATRIX_PATH, 'xb') as f:
        p.dump(to_write, f)

    return to_write


def mat_name_hash(fold_spec, county) -> str:
    """Use adler32 to get a hash used to name the training and validation
    smaller matrices / dataframes.

    Args:
    ---
    fold_spec: one of the fold outputs from time_splitter::get_time_split
    """
    data_to_hash = (str(fold_spec) + str(county)).encode('utf-8')
    return str(zlib.adler32(data_to_hash) & 0xffffffff)


def get_county_columns(config, county):
    """Return the relevant numerical and categorical columns for the desired
    county. """
    # TODO: Implement
    db_conn = get_database_connection()
    table_names = []
    for table_name in config['features']:
        if config['features'][table_name]['county'] in [county, 'both']:
            table_names.append(table_name)
    column_names = []
    columns_to_exclude = set(['joid', 'as_of_date'])
    for table_name in table_names:
        for actual_table_name in [table_name + end for end in ['_num', '_cat']]:
            # actual_table_name is the name of the table in the features schema
            column_names += list(get_column_names(db_conn, actual_table_name, schema='features'))

    return set(column_names) - columns_to_exclude


def load_labels(db_conn, county, folds_spec, label_tablename):
    """ This function can return a dict with fold_spec keys and the label of
    interest.
    """
    county_str = "'doco', 'joco'" if county == 'both' else "'" + county + "'"

    labels_dict = {}
    for fold in folds_spec:
        train_dates, validate_date = fold
        all_dates = set(list(train_dates) + [validate_date])

        all_dates_str = '(' + ', '.join([f"'{d}'" for d in all_dates]) + ')'

        query = f'''
        select joid, as_of_date, label
            from modeling.{label_tablename}
        where as_of_date in {all_dates_str}
            and county in({county_str});
        '''

        df_label = pd.read_sql(query, db_conn, index_col=['joid', 'as_of_date'])

        train_slice = (slice(None), train_dates)
        val_slice = (slice(None), [validate_date])
        train_labels = df_label.loc[train_slice, :]
        val_labels = df_label.loc[val_slice, :]

        labels_dict[(fold, county)] = (train_labels, val_labels)

    return labels_dict


def write_small_mats(config, df, num_columns, cat_columns, fold_spec, county, county_columns):
    """Compute (if necessary) smaller training and validation matrices and save
    to disk. Will write a tuple of train and validation matrix / datframe, in
    that order.

    Args:
    ---
    config: loaded config.yaml
    df: master_df as returned by get_master_df()['master_df']
    num_columns: names of numerical feature columns
    cat_columns: names of categorical feature columns
    fold_spec: single fold, from time_splitter::get_time_split()
    county: county string
    county_columns: columns relevant to the selected county
    """
    # Check if matrices already exist
    matrices_path = os.path.join(SMALL_MATRICES_DIR, mat_name_hash(fold_spec, county))
    if os.path.exists(matrices_path):
        print(f'matrix for fold {fold_spec} and county {county} already exists. Loading...')
        return

    print('Writing small train / val matrices')

    train_dates, validate_date = fold_spec
    # TODO: instead of these slices, consider using df.index.get_level_values_of('as_of_date').isin(train_dates)
    train_slice = (slice(None), list(train_dates))
    none_slice = (slice(None), slice(None))  # silly but necessary for the multi-index
    validate_slice = (slice(None), [validate_date])
    train_df = pd.DataFrame(df.loc[train_slice, :])
    validate_df = pd.DataFrame(df.loc[validate_slice, :])

    # Check that the dataframes are not empty; this error is usually caused by requesting missing dates
    error_message = """It is likely that a train or validation date that does not appear in
        master_features was requested. Check the as_of_dates in features and cohort."""
    if len(train_df.index) == 0:
        raise Exception(error_message)
    if len(validate_df.index) == 0:
        raise Exception(error_message)

    train_idx, validate_idx = train_df.index, validate_df.index

    # If county is not both then drop irrelevant feature columns
    # NOTE: due to the next few lines, the matrices columns orders vary accross different small mat pairs.
    # They are the same for each training and validation pair, however.
    if county != 'both':
        num_columns = list(set(num_columns) & county_columns)
        cat_columns = list(set(cat_columns) & county_columns)

    # Impute numerical features
    # NOTE: this only imputes age
    # TODO: Add a check for imputation of other numerical features if more are to be added
    train_mean_age = train_df['dem_age'].mean()
    train_df.loc[none_slice, ['dem_age']] = train_df['dem_age'].fillna(train_mean_age)
    validate_df.loc[none_slice, ['dem_age']] = validate_df['dem_age'].fillna(train_mean_age)

    # Fit one hot encoder to categorical training features and transform
    # categorical features for train and validate
    encoder = get_one_hot_encoder()
    encoder.fit(train_df[cat_columns])
    onehot_cat_columns = encoder.get_feature_names_out(cat_columns)
    train_cat_df = pd.DataFrame(encoder.transform(train_df[cat_columns]), columns=onehot_cat_columns, index=train_idx, dtype=np.float32)
    validate_cat_df = pd.DataFrame(encoder.transform(validate_df[cat_columns]), columns=onehot_cat_columns, index=validate_idx, dtype=np.float32)

    train_df = train_cat_df.join(train_df[num_columns])
    validate_df = validate_cat_df.join(validate_df[num_columns])

    # Standardize features
    train_df, validate_df = standardize_data(train_df, validate_df)

    # create small mats' directory if it does not exist
    if not os.path.exists(SMALL_MATRICES_DIR):
        os.mkdirs(SMALL_MATRICES_DIR)

    # Write files
    filename = mat_name_hash(fold_spec, county)
    path = os.path.join(SMALL_MATRICES_DIR, filename)
    to_write = train_df, validate_df
    with open(path, 'wb') as f:
        p.dump(to_write, f)


def write_matrices_from_master_df(config: dict, df: pd.DataFrame, num_columns: list[str], cat_columns: list[str], fold_specs: list, county: str):
    """ Use the master dataframe to write the train and validation dataframes
    to disk for each combination of fold and county.

    Args:
    ---
    config: config.yaml dictionary
    df: master_df from get_master_df()
    num_columns: numerical feature column names
    cat_columns: categorical feature column names
    fold_specs: single fold form output of time_splitter::get_time_split()
    county: county
    """
    # TODO: Can add a further sanity check asserting all train / validate dates are in the index of master_df
    county_columns = get_county_columns(config, county) if county != 'both' else []
    for i, fold_spec in enumerate(fold_specs):
        print(f'Writing small mat for {county} fold {i} of {len(fold_specs)}')
        write_small_mats(config, df, num_columns, cat_columns, fold_spec, county, county_columns)


def check_all_small_mats(fold_specs, county):
    """Sanity checking function to ensure that all small matrices were saved to
    disk. """
    res = []
    # Test matrices were saved
    for fold_spec in fold_specs:
        filename = mat_name_hash(fold_spec, county)
        path = os.path.join(SMALL_MATRICES_DIR, filename)
        res.append((fold_spec, county, os.path.exists(path)))
    return res


def write_matrix_driver(db_conn, config, make_master_df_only=False, cherry_picked_folds=None):
    """Driver function to write, if necessary, all matrices.

    Args:
    ---
    db_conn: database connection engine
    config: config.yaml dict
    make_master_df_only: for testing/ ase purposes will only write the master_df
        to disk and then error out
    cherry_picked_folds: for testing purposes, use only a few cherry picked folds
    """
    print('start')
    start = time()
    fold_specs = get_time_split(config)
    if cherry_picked_folds is not None:
        fold_specs = cherry_picked_folds

    train_dates, validate_dates = get_train_and_val_dates(fold_specs)
    matrices_dict = get_master_df(db_conn, train_dates, validate_dates)
    print(f'loaded master df in {(time() - start)/60:.2} mins')
    if make_master_df_only:
        raise Exception('Done creating master_df; intentionally erroring to exit code.')

    master_df = matrices_dict['master_df']
    num_columns = matrices_dict['num_columns']
    cat_columns = matrices_dict['cat_columns']

    county = config['county']
    print('Using counties ', county)

    # Write the small training and validation matrices
    write_matrices_from_master_df(config, master_df, num_columns, cat_columns, fold_specs, county)
    print(f'created / loaded small mats in {(time() - start)/60:.2} mins')


def load_matrices(county, fold_specs):
    """Load matrices for all fold_specs and the singular given county
    ('joco', 'doco', or 'both').
    """
    matrices = {}
    for fold_spec in fold_specs:
        filename = mat_name_hash(fold_spec, county)
        path = os.path.join(SMALL_MATRICES_DIR, filename)
        with open(path, 'rb') as f:
            matrices[(fold_spec, county)] = p.load(f)
    return matrices
