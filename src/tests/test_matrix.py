import pandas as pd
import datetime 
import pytest
import pipeline.matrix as matrix
from email import utils
from pipeline.matrix import get_matrices, get_master_df
from utils.helpers import get_database_connection, get_all_table_names
from pipeline.time_splitter import get_all_dates
from datetime import date


# Define fixtures
# If this code breaks, check that the dates in this fixture are a part of the fratures and labels tables
@pytest.fixture
def train_and_validate_dates():
    """ date_tuples is a list of (train_date, validate_date) tuples."""
    date_tuples = []
    train_dates, validate_date = ([datetime.date(2019, 10, 1), datetime.date(2020, 1, 1)]), datetime.date(2020, 10, 1)
    # Can use a for loop to add many more dates to test different cohorts, etc.
    date_tuples.append((train_dates, validate_date))
    return date_tuples


@pytest.fixture
def db_conn():
    return get_database_connection()


@pytest.fixture
def num_features(db_conn, train_and_validate_dates):
    all_dates = get_all_dates(train_and_validate_dates)
    all_dates_str = '(' + ', '.join([f"'{d}'" for d in all_dates]) + ')'
    # Brought in from some config 
    features_schema = 'features'
    feat_table_names = get_all_table_names(db_conn, features_schema)
    print(feat_table_names)
    tot_cat, tot_num = 0, 0  # total number of categorical and numerical features
    for tn in feat_table_names:
        q = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{features_schema}'
        AND table_name = '{tn}'
        """
        columns = pd.read_sql(q, db_conn)['column_name'].values.tolist()
        columns.remove('joid')
        columns.remove('as_of_date')
        if tn.endswith('_cat'):
            distinct_vals_str = ', '.join([f'count(distinct {feat_name}) + count(distinct case when {feat_name} is null then 1 end) as {feat_name}' for feat_name in columns])
            q = f"""
            select {distinct_vals_str}
            from {features_schema}.{tn}
            where as_of_date in {all_dates_str}
            """
            # sum of different features per column
            tot_cat += pd.read_sql(q, db_conn).sum(axis=1).sum()
        elif tn.endswith('_num'):
            tot_num += len(columns)

    return tot_cat, tot_num


def test_dimensions(db_conn, train_and_validate_dates, num_features):
    """ Ensure the dimensions for the output matrices correctly correspond to the
    number of features and person-dates. """
    tot_cat, tot_num = num_features
    for train_dates, validate_date in train_and_validate_dates:
        X_train, y_train, X_test, y_test, _ = get_matrices(db_conn, train_dates, validate_date)
        # assert (X_train.shape[1] == num_features and X_test.shape[1] == num_features)
        assert X_train.shape[0] == y_train.shape[0]
        assert X_test.shape[0] == y_test.shape[0]
        # # Check that both of the labels are indeed 1-d vectors
        assert y_train.shape[1] == 1 and y_test.shape[1] == 1

        # Check the number of features
        print('tot_cat ', tot_cat)
        print('tot_num ', tot_num)
        print('X_train shape: ', X_train.shape)
        print('X_test shape: ', X_test.shape)
        assert all(X.shape[1] == tot_cat + tot_num for X in [X_train, X_test])

        print('X_train shape: ', X_train.shape)
        print('X_test shape: ', X_test.shape)
        print('total people ', X_train.shape[0] + X_test.shape[0])


def test_get_all_dates_master():
    train_dates = [
        (date(19, 1, 1), date(19, 7, 1)),
        (date(18, 7, 1), date(19, 1, 1))
    ]
    validate_dates = [date(20, 1, 1), date(19, 7 , 1)]
    true_all_dates = set([date(19, 1, 1), date(19, 7, 1), date(18, 7, 1), date(19, 1, 1), date(20, 1, 1), date(19, 7 , 1)])
    true_train_dates = set([date(18, 7, 1), date(19, 1, 1), date(19, 7, 1)])
    true_validate_dates = set(validate_dates)

    train_dates_tocheck, validate_dates_tocheck, all_dates_tocheck = matrix.get_all_dates_master(train_dates, validate_dates)
    assert set(all_dates_tocheck) == true_all_dates
    assert set(train_dates_tocheck) == true_train_dates
    assert set(validate_dates_tocheck) == true_validate_dates


def test_get_master_df(db_conn):
    assert True
    pass
    params = {
        'train_dates': [
            [date(2017, 1, 1), date(2017, 4, 1)],
            [date(2017, 4, 1), date(2017, 7, 1)]
        ],
        'validate_dates': [date(2017, 10, 1), date(2018, 1, 1)],
        'label_names': ['DEATH BY SUICIDE, DEATH BY OVERDOSE, SUICIDE ATTEMPT AMBULANCE RUN, SUICIDAL AMBULANCE RUN, SUBSTANCE USE AMBULANCE RUN, OTHER BEHAVIORAL CRISIS AMBULANCE RUN']
    }
    df = matrix.get_master_df(db_conn, **params)
    df.to_pickle('/mnt/data/projects/dojo-mh/matrices/small-test-matrices.p')
    print(df.shape)
    # joids = df.index.get_level_values(0)
    as_of_dates = df.index.get_level_values(1)
    true_as_of_dates = [date(2017, 1, 1), date(2017, 4, 1), date(2017, 7, 1), date(2017, 10, 1), date(2018, 1, 1)]
    assert set(as_of_dates) == set(true_as_of_dates)
