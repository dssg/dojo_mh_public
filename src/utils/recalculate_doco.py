'''
We had a bug that saved the precision / recall for DoCo at k = 75 as k = 40 rather than calculating the
k = 40 precision / recall and saving that one. This script recreates the correct k = 40 precision / recall
from the models that were saved during the various model runs and updates the results.test_evaluations
table in the database. For the models that were not saved to disk we cannot do this, and we therefore
delete the incorrect precision / recall in results.test_evaluations
'''
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from utils.helpers import get_database_connection
from postmodeling.evaluation import get_test_pred_labels_from_csv, _calculate_metric


def get_precision_recall(model_id, county='doco', county_k=40):

    try:
        df_test = get_test_pred_labels_from_csv(model_id)
    except:
        print('Could not read predictions')
        return 

    # Get the evaluation for this model id
    df = _calculate_metric(df_test, county=county)
    df = df[df['county_k'] == county_k]
    colnames = ['model_id', 'county', 'as_of_date', 'metric', 'k', 'county_k', 'county_value']
    df = df[colnames]
    df = df.rename(columns={'county_value': 'value'})
    return df


def save_correct_precision_recall(db_conn):
    # Get all the model ids for DoJo
    query = '''
        select * from results.test_evaluations te
        join results.models m
        using(model_id)
        join results.model_sets ms
        using(model_set_id)
        join results.experiments e
        using(experiment_id)
        where county = 'doco' and county_k = 40;
    '''

    df = pd.read_sql(query, db_conn)
    model_ids = df.model_id.unique()[::-1]

    res = Parallel(90)(
        delayed(get_precision_recall)(
            id
        ) for id in model_ids
    )

    # Save newly calculated precision / recall to a separate database
    res.to_sql(
        name='test_evaluations_doco_fixed', con=db_conn, schema='results',
        index=False, if_exists='replace'
    )


def update_evaluations_table(df, operation='update'):
    for _, row in df.iterrows():
        model_id = row['model_id']
        county = row['county']
        county_k = row['county_k']
        as_of_date = row['as_of_date']
        metric = row['metric']
        value = row['value']

        if operation == 'update':
            query = f"""
                update results.test_evaluations
                set value = {value}
                where model_id = {model_id}::int
                and county = '{county}'
                and county_k = {county_k}
                and as_of_date = '{as_of_date}'::date
                and metric = '{metric}';
            """
        
        if operation == 'delete':
            query = f"""
                delete from results.test_evaluations
                where model_id = {model_id}::int
                and county = '{county}'
                and county_k = {county_k}
                and as_of_date = '{as_of_date}'::date
                and metric = '{metric}';
            """

        db_conn.execute(query)
        db_conn.execute("COMMIT")


db_conn = get_database_connection()

#save_correct_precision_recall(db_conn)
# The test_evaluations_doco_fixed is created by running the line above
df_fixed = pd.read_sql('select * from results.test_evaluations_doco_fixed;', db_conn)
df_old = pd.read_sql('''select * from results.test_evaluations where county = 'doco';''', db_conn)

# np.isnan(df_fixed.value) # 25, recall should be 0 not NaN (fixed function later to yield 0)
df_fixed['value'] = df_fixed['value'].fillna(0)

df = pd.merge(df_fixed, df_old, on=['model_id', 'county', 'metric', 'county_k'])

# Find those entries that differ
sel = np.where((df['value_x'] != df['value_y']))[0]
df = df.iloc[sel].reset_index()
df = df.rename(columns={'as_of_date_x': 'as_of_date', 'value_x': 'value'})

# Update results.test_evaluations
#update_evaluations_table(df, operation='update')

# Find model ids that are in df_old but not in df_fixed
in_old_not_in_new = np.array(list(set(df_old.model_id.unique()).difference(set(df_fixed.model_id.unique()))))
df_incorrect = df_old[df_old.model_id.isin(in_old_not_in_new)]

# Remove these entries from results.test_evaluation since they are incorrect
#update_evaluations_table(df_incorrect, operation='delete')