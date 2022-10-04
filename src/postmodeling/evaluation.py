"""
Model evaluation functions.
"""
import os
import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
from utils.helpers import get_database_connection
from utils.constants import PREDICTIONS_DIR, LABEL_MAPPING


def get_best_modelsets(
    db_conn, county='joco', metric='precision',
    finished=False, model_types=None, months_future=None,
    rank_on='regret', min_dates=2, top=1,
    exclude_types=['FeatureRanker', 'LinearRanker']
    ):
    """Returns dataframe of top model sets across label groups where
       performance is assessed with respect to the best case (regret) or the (average) metric.

    Args:
        db_conn (sqlalchemy database connection)
        county (str, 'joco'): county, defaults to 'joco'
        metric (str, 'precision'): metric to rank on, defaults to precision
        finished (boolean, False): whether to only look at finished experiments, defaults to False
        model_types (list, None): List of strings indicating the model types to assess, defaults to None (which means all)
        months_future (str, None): string indicating the months in the future to select on, defaults to None (which means all)
        rank_on (str, 'regret'): metric to rank by, defaults to first using regret
        min_dates (int, 2): the minimum number of validation splits a model set should have completed to be included in selection
        top (int, 1): number of model sets to return, defaults to 1
        exclude_types (list): what types of model sets should be excluded in the model selection
    
    Returns:
        dataframe of top X model sets across label groups with highest regret / average value of metric
    """
    best = []
    label_groups = LABEL_MAPPING.keys()

    # Hacky cashing to make running of final evaluation notebook quicker ...
    dir = '/mnt/data/projects/dojo-mh/best-models'
    if model_types:
        type = 'best_baselines'
    else:
        type = 'best_models'

    filename = '_'.join([
        type, county, metric, rank_on, 'top',
        str(min_dates), str(finished), str(top) + '.csv']
    )
    filedir = os.path.join(dir, filename)

    if os.path.exists(filedir):
        print('Best models file exists, reading from disk ...')
        return pd.read_csv(filedir)
    
    # Get top models for each label group
    for lg in label_groups:
        df = rank_models(
                db_conn, label_group=lg, county=county, metric=metric,
                finished=finished, model_types=model_types,
                months_future=months_future, min_dates=min_dates,
                rank_on=rank_on, exclude_types=exclude_types
            )
        best.append(df.head(top))
    
    df = pd.concat(best)
    df.to_csv(filedir, index=False)

    return df


def rank_models(
    db_conn, label_group='Potentially fatal',
    county='joco', metric='precision',
    finished=False, model_types=None,
    months_future=None, rank_on='regret',
    min_dates=2, exclude_types=None
    ):
    """Ranks model sets we ran so far according to performance where
       performance is assessed with respect to the best case (regret) or the (average) metric.

    Args:
        db_conn (sqlalchemy database connection)
        label_group (str): label group, e.g. 'Potentially fatal', see utils.constants
        county (str, 'joco'): county, defaults to 'joco'
        metric (str, 'precision'): metric to rank on, defaults to precision
        finished (boolean, False): whether to only look at finished experiments, defaults to False
        model_types (list, None): List of strings indicating the model types to assess, defaults to None (which means all)
        months_future (str, None): string indicating the months in the future to select on, defaults to None (which means all)
        rank_on (str, 'regret'): metric to rank by, defaults to first using regret
        top (int, 1): number of model sets to return, defaults to 1
        min_dates (int, 2): the minimum number of validation splits a model set should have completed to be included in selection
        exclude_types (list, None): what types of model sets should be excluded in the model selection
    
    Returns:
        dataframe sorted by regret / average value of metric
    """

    assert rank_on in ['regret', 'metric']

    if county == 'doco':
        earliest_date = '2019-09-01'
    else:
        earliest_date = '2017-12-01'


    # Get relevant details from the database
    # TODO: To change once results.test_evaluations is fixed for DoCo results
    tablename = 'test_evaluations' if county == 'joco' else 'test_evaluations_doco_fixed'
    query = f'''
    select * from results.{tablename} te
        join results.models m
        using(model_id)
        join results.model_sets ms
        using(model_set_id)
        join results.experiments e
        using(experiment_id)
        where metric='{metric}'
        and county = '{county}'
        and label_group = '{label_group}'
        and as_of_date >= '{earliest_date}'::date;
    '''

    df = pd.read_sql(query, db_conn)

    # Select only the experiments with labels 'months_future' in the future
    if months_future: 
        df = df[df.apply(lambda x: x['config']['labels']['months_future'] == months_future, axis=1)]
    
    # Only select among models with all as of dates
    if min_dates:
        sel = df.groupby(['model_set_id']).value.transform('count') >= min_dates
        df = df[sel]

    # Model types gets priority over excluded types
    if sorted(model_types) == sorted(exclude_types):
        exclude_types = None

    # Only get the relevant model types
    if model_types:
        model_types_str = '|'.join(model_types)
        df = df[df.type.str.contains(model_types_str)]

    # Remove exluded types
    if exclude_types:
        exclude_types_str = '|'.join(exclude_types)
        df = df[~df.type.str.contains(exclude_types_str)]

    # Get only the relevant k
    county_k = 75 if county == 'joco' else 40
    df = df[df['county_k'] == county_k]

    # Compute regret (average of difference to best case)
    df['regret_per_as_of_date'] = df.groupby(['as_of_date']).value.transform('max') - df.value
    df['regret'] = df.groupby(['model_set_id']).regret_per_as_of_date.transform('mean')
    df.drop(columns=['regret_per_as_of_date'], inplace=True)

    # Compute average metric across as of dates for each model set
    key = 'mean_' + metric
    df[key] = df.groupby(['model_set_id']).value.transform('mean')

    # Drop multiple models per model set since we care about averages
    df.drop_duplicates(subset=['model_set_id'], inplace=True)

    # Keep only those columns
    colnames = [
        'experiment_id', 'model_set_id', 'label_group', 'county', 'trained_on',
        'metric', key, 'regret', 'type', 'params', 'starts', 'ends', 'config'
    ]

    df = df[colnames]

    # Only look at experiments that successfully finished
    if finished:
        df = df[~pd.isnull(df['ends'])]
    
    # If we rank on regret, then use average metric to break ties (and vice versa)
    if rank_on == 'regret':
        sort_by = ['regret', key]
        ascending = [True, False]
    else:
        sort_by = [key, 'regret']
        ascending = [False, True]
    
    return df.sort_values(by=sort_by, ascending=ascending)


def get_test_pred_labels(db_conn, model_id):
    """
    Returns model results in test_predictions table given model_id. 

    Args:
        - db_conn (sqlalchemy.engine.base.Connection): database connection
        - model_id (int)

    Returns:
        - pandas dataframe
    """

    query = f'''
        select *
        from results.test_predictions tp
        where tp.model_id = {model_id};
    '''
    formatted_query = query.format(model_id=model_id)
    return pd.read_sql(formatted_query, db_conn)


def get_test_pred_labels_from_csv(model_id, predictions_dir=PREDICTIONS_DIR):
    """
    Returns model results from csv file in predictions directory given model_id. 

    Args:
        - model_id (int)
        - predictions_dir (str): path to predictions directory

    Returns:
        - pandas dataframe
    """

    filedir = os.listdir(predictions_dir)
    file = [x for x in filedir if '_' + str(model_id) + '.csv' in x]
    if file != []:
        file = file[0]
    else:
        raise Exception('Predictions do not exist for this model!')
    filepath = os.path.join(predictions_dir, file)
    
    return pd.read_csv(filepath)


def get_model_info(db_conn, model_id) :
    '''
    Print information about the model being run.

     Args:
        - db_conn (sqlalchemy.engine.base.Connection): database connection
        - model_id (int)
    '''

    formatted_query = '''
    select * from results.model_sets
        where model_set_id = (
            select model_set_id
            from results.models
            where model_id = {model_id}
        );
    '''.format(model_id=model_id)

    model_info = pd.read_sql(formatted_query, db_conn)
    
    info_string = f'''
        model: {str(model_id)}  
        model set: {str(model_info['model_set_id'].values[0])}, 
        model type: {str(model_info['type'].values[0])}, 
        params = {str(list[model_info['params'][0]])[5:-1]}
    '''

    print(info_string)


def get_evaluation(db_conn, model_id, read_from_db=False):
    '''
    Calculate the precision and recall for a model across counties and per county

    Args:
        - db_conn (sqlalchemy.engine.base.Connection): database connection
        - model_id (int): model id
    
    Returns:
        - evaluations_df (pandas dataframe): df holding relevant model, label, county information
                                             and precision and recall
    '''
    # Get test predictions from database
    if read_from_db:
        formatted_query = f'''
        select * from results.test_predictions
        where model_id = {model_id};
        '''
        df = pd.read_sql(formatted_query, db_conn)

    # Get test predictions from /mnt/data
    else:
        filedir = os.listdir(PREDICTIONS_DIR)
        file = [x for x in filedir if '_' + str(model_id) + '.csv' in x]
        if file != []:
            file = file[0]
        else:
            raise Exception('Predictions do not exist for this model!')
        filepath = os.path.join(PREDICTIONS_DIR, file)
        df = pd.read_csv(filepath)

    # We have both counties in the predictions
    counties = df['county'].unique()

    assert counties.size < 3

    if counties.size == 2:
        # add overall precision and recall
        df_all = _calculate_metric(df, county=None)

        # calculate county-specific precision and recall, join them
        df_joco = _calculate_metric(df, county='joco')
        df_doco = _calculate_metric(df, county='doco')
        df_counties = pd.concat([df_joco, df_doco])

        # sort each according to k, which spans counties, then add county_values
        df_all = df_all.sort_values(by=['joid', 'metric'], ascending=True)
        df_counties = df_counties.sort_values(by=['joid', 'metric'], ascending=True)

        # insert county specific precision and recall
        df_all.insert(df_all.shape[1], 'county_value', np.array(df_counties['county_value']))
    
    # If only one element, calculate metrics only for that county
    else:
        df_all = _calculate_metric(df, county=counties[0])
    
    return df_all


def _calculate_metric(df, county=None, melt=True):
    """Calculates precision and recall

    Args:
        df (dataframe): dataframe read from results.test_predictions
        county (string, None): which county to compute the metrics for
        melt (bool, True): if the resulting dataframe should be melted to long format

    Returns:
        df: dataframe
    """

    if county:
        df = df[df['county'] == county]

    sort_by = 'county_k' if county else 'k'
    df = df.sort_values(by=sort_by, ascending=True)    

    # calculate overall precision and recall
    cumulative_sum = np.array(np.cumsum(df['label']))

    df['precision'] = cumulative_sum / df[sort_by]
    df['recall'] = cumulative_sum / cumulative_sum[-1]

    id_vars = [
        'model_id', 'joid', 'county', 'as_of_date',
        'score', 'label_name', 'label', 'k','county_k'
    ]

    if melt:
        # melt to long format
        df = df.melt(
            id_vars=id_vars,
            value_vars=['precision', 'recall'],
            var_name='metric',
            value_name='value'
        )

        if county:
            df = df.rename(columns={'value': 'county_value'})

    return df


def get_model_ids(db_conn):
    '''
    Get all model ids in the results table.

    Args:
        - db_conn (sqlalchemy.engine.base.Connection): database connection
    
    Returns: 
        - model_id_list (list): list of model ids
    '''

    sql_query = "select distinct(model_id) from results.test_predictions;"
    model_id_list = pd.read_sql(sql_query, db_conn)
    model_id_list = list(model_id_list['model_id'])

    return model_id_list


def get_all_evaluations(db_conn, model_id_list):
    '''
    Concatenate the table with calculated metrics into one larger table

    Args:
        - db_conn (sqlalchemy.engine.base.Connection): database connection
        - model_id_list 
    
    Returns:
        - evaluations_all_df (pandas dataframe): 
    '''
    evaluation_tables_list = []

    # Loop through each model and calculate precision/recall
    for model_id in model_id_list :
        temp_df = get_evaluation(db_conn, model_id) 
        evaluation_tables_list.append(temp_df)
        
    evaluations_all_df = pd.concat(evaluation_tables_list, axis=0)

    return evaluations_all_df


def get_confusion_matrix(model_results, joco_k=None, doco_k=None, total_k=None):
    """
    Return confusion matrix. 

    Args:
        - model_results (Pandas Dataframe): table containing scores and labels
        - joco_k (int, None): top k scores at which evaluation is done for Johnson county 
        - doco_k (int, None): top k scores at which evaluation is done for Douglas county 
        - total_k (int, None): top k scores at which evaluation is done for both counties
    """

    if not total_k:
        assert not (joco_k is None and doco_k is None)

        if not joco_k:
            model_results = model_results[model_results['county'] != 'joco']
        if not doco_k:
            model_results = model_results[model_results['county'] != 'doco']

        # Return 1 when k smaller than specified k either in DoCo or JoCo
        y_pred = np.where(
            np.logical_or(
                np.logical_and(model_results["county"] == 'doco', model_results["county_k"] <= doco_k),
                np.logical_and(model_results["county"] == 'joco', model_results["county_k"] <= joco_k)
            ), 1, 0)
    else:
        assert total_k is not None

        y_pred = np.where(model_results["k"] <= total_k, 1, 0)


    y_true = model_results['label']
    return confusion_matrix(y_true, y_pred, labels=[0, 1])


def plot_confusion_matrix(conf_matrix):
    """
    Return plot of confusion matrix. 

    Args:
        - confusion_matrix (sklearn metric)
    """

    return ConfusionMatrixDisplay(conf_matrix, display_labels=[0, 1])


def get_features_test_pred(
    db_conn, model_id, features_table,
    joco_k=None, doco_k=None, total_k=None, read_from_db=False
    ):
    """
    Read features and labels of a given features table.

    Args:
        - db_conn (sqlalchemy.engine.base.Connection): database connection
        - model_id (int)
        - features_table (str): name of relevant features table
        - joco_k (int, None): top k scores at which evaluation is done for Johnson county 
        - doco_k (int, None): top k scores at which evaluation is done for Douglas county 
        - total_k (int, None): top k scores at which evaluation is done for both counties
    
    Returns Pandas Dataframe including features, score, k, and label. 
    """

    if read_from_db:
        query = f'''
        select 
        f.*,
        tp.county,
        tp.score,
        tp.label,
        tp.k,
        tp.county_k
        from results.test_predictions tp
        left join features.{features_table} f
        on tp.joid = f.joid
        and f.as_of_date = tp.as_of_date     
        where model_id = {model_id};
        '''
        formatted_query = query.format(model_id=model_id, features_table=features_table)
        features_preds = pd.read_sql(formatted_query, db_conn)
    
    else:
        preds = get_test_pred_labels_from_csv(model_id)
        as_of_dates = preds['as_of_date'].unique()
        str_dates = ','.join(["'" + str(date) + "'" for date in as_of_dates])

        query = f'''
        select * from features.{features_table} f
        where as_of_date in({str_dates});
        '''

        features_df = pd.read_sql(query, db_conn)
        preds['as_of_date'] = pd.to_datetime(preds['as_of_date'], format='%Y-%m-%d')
        features_df['as_of_date'] = pd.to_datetime(features_df['as_of_date'], format='%Y-%m-%d')

        features_preds = pd.merge(
            features_df, preds, how='inner', on=['joid', 'as_of_date']
        )

    if not total_k:
        assert not (joco_k is None and doco_k is None)

        if not joco_k:
            features_preds = features_preds[features_preds['county'] != 'joco']
        if not doco_k:
            features_preds = features_preds[features_preds['county'] != 'doco']
    
        # Return 1 when k smaller than specified k either in DoCo or JoCo
        features_preds["pred"] = np.where(
            np.logical_or(
                np.logical_and(features_preds["county"] == 'doco', features_preds["county_k"] <= doco_k),
                np.logical_and(features_preds["county"] == 'joco', features_preds["county_k"] <= joco_k)
            ), 1, 0)
    else:
        assert total_k is not None

        features_preds["pred"] = np.where(features_preds["k"] <= total_k, 1, 0)

    return features_preds


def create_crosstabs(features_preds, column_name, split_tuple=None):
    """
    Create a crosstabs of a feature and label and prediction. 

    Args:
        - features_preds (Pandas Dataframe): table with features and predictions
        - column_name (str): column in dataframe to get crosstab for
        - split_tuple (tuple, None): list of bins (floats) and list of names (str) 
                                     to split numerical column on
    
    Returns tuple of two Pandas Dataframes of feature with labels and predictions
    """

    feat_preds = features_preds.copy()

    # Split numerical variables into categories
    if split_tuple:
        bins, names = split_tuple
        feat_preds["split_col"] = pd.cut(feat_preds[column_name], bins, labels=names)
        column_name = "split_col"
        feat_preds["split_col"] = feat_preds["split_col"].astype(str)

    label_crosstab = pd.crosstab(feat_preds.label, feat_preds[column_name])
    pred_crosstab = pd.crosstab(feat_preds.pred, feat_preds[column_name])

    return (label_crosstab, pred_crosstab)
    

def get_predictions(evaluations_df, joco_k=75, doco_k=40):
    """Adds predictions to the evaluations_df
    Args:
        - evaluations_df (dataframe): evaluations data from get_evaluation
        - joco_k (int, 75): Johnson county k
        - doco_k (int, 40): Douglas county k
    """

    # Return 1 when k smaller than specified k either in DoCo or JoCo
    y_pred = np.where(
        np.logical_or(
            np.logical_and(evaluations_df["county"] == 'doco', evaluations_df["county_k"] <= doco_k),
            np.logical_and(evaluations_df["county"] == 'joco', evaluations_df["county_k"] <= joco_k)
        ), 1, 0)

    evaluations_df['prediction'] = y_pred
    return evaluations_df


def get_split_label_df(db_conn, model_ids, joco_k=75, doco_k=40):
    """Returns the evaluations df merged with the split labels
    Args:
        - evaluations_df (dataframe): evaluations data from get_evaluation
        - joco_k (int, 75): Johnson county k
        - doco_k (int, 40): Douglas county k
    """

    str_model_ids = ','.join([str(id) for id in model_ids])

    query = f'''
    select tp.*, sl.label_name as split_label_name, sl.label as split_label
    from modeling.split_labels sl
    left join results.test_predictions tp
    on sl.joid = tp.joid and sl.as_of_date = tp.as_of_date
    where sl.label and tp.label and tp.model_id in({str_model_ids});
    '''

    df = pd.read_sql(query, db_conn)
    df = get_predictions(df, joco_k=joco_k, doco_k=doco_k)
    return df


def get_model_info_from_experiment_ids(db_conn, experiment_ids):
    """Get models info from given experiment ids. 
    Args: 
        - db_conn: database connection
        - experiment_ids (list of int)
    
    Returns: Pandas dataframe
    """
    exp_ids = ', '.join([str(e) for e in experiment_ids])

    sql_q = f"""
        select 
            experiment_id, 
            model_set_id, 
            model_id, 
            train_end_date,
            type,
            trained_on, 
            label_group
        from results.models
        left join results.model_sets ms 
            using(model_set_id)
        left join results.experiments e
            using(experiment_id)
        where experiment_id in ({exp_ids});
    """

    df = pd.read_sql(sql_q, db_conn)

    return df


def get_models_info(db_conn, model_ids):
    """Get models information, including label_group.
    Args: 
        - db_conn: database connection
        - model_ids (list of int)
    
    Returns: Pandas dataframe
    """
    mod_ids = ', '.join([str(m) for m in model_ids])

    sql_q = f"""
        select experiment_id, 
            model_set_id, 
            model_id, 
            train_end_date,
            type,
            trained_on, 
            label_group
        from results.models
        left join results.model_sets ms 
            using(model_set_id)
        left join results.experiments e
            using(experiment_id)
        where model_id in ({mod_ids});
    """

    return pd.read_sql(sql_q, db_conn)


def get_test_pred_labels_from_csv(model_id, predictions_dir=PREDICTIONS_DIR):
    """
    Returns model results from csv file in predictions directory given model_id. 
    Args:
        - model_id (int)
        - predictions_dir (str): path to predictions directory
    Returns:
        - pandas dataframe
    """

    filedir = os.listdir(predictions_dir)
    file = [x for x in filedir if '_' + str(model_id) + '.csv' in x]
    if file != []:
        file = file[0]
    else:
        raise Exception('Predictions do not exist for this model!')
    filepath = os.path.join(predictions_dir, file)

    return pd.read_csv(filepath)


def get_test_pred_labels_from_csv(model_id, predictions_dir=PREDICTIONS_DIR):
    """
    Returns model results from csv file in predictions directory given model_id.
    Args:
        - model_id (int)
        - predictions_dir (str): path to predictions directory
    Returns:
        - pandas dataframe
    """
    filedir = os.listdir(predictions_dir)
    file = [x for x in filedir if '_' + str(model_id) + '.csv' in x]
    if file != []:
        file = file[0]
    else:
        raise Exception('Predictions do not exist for this model!')
    filepath = os.path.join(predictions_dir, file)
    return pd.read_csv(filepath)

def create_split_label_counts(db_conn) :
    '''
    Create a table to see counts of split labels by as_of_date and label_name
    '''
    
    query = '''
    set role 'dojo-mh-role';
    drop table if exists modeling.split_labels_counts;
    create table modeling.split_labels_counts as
    with labels_c as (
        select c.joid,
            c.as_of_date,
            c.county,
            label_name,
            label
        from 
            modeling.cohort c 
        join modeling.split_labels s
            on c.joid = s.joid and 
            c.as_of_date = s.as_of_date
        ) 
        select
            as_of_date,
            label_name,
            count(distinct(joid))
        from labels_c 
        where label = true
        group by as_of_date, label_name
        '''

    db_conn.execute(query)
    db_conn.execute("COMMIT")


if __name__ == "__main__":
    db_conn = get_database_connection()
    
    # Params
    model_id = 433
    doco_k = 40
    joco_k = 75

    # Confusion matrix
    model_results = get_test_pred_labels(db_conn, model_id)

    if len(model_results) > 0:
        cf = get_confusion_matrix(model_results, doco_k=doco_k, joco_k=joco_k)
        plt_cf = plot_confusion_matrix(cf)
        plt_cf.plot()
    else: 
        print("No results available from selected model_id: ", model_id)
    
    # Crosstabs: categorical demographics
    features_table = 'demographics_cat'
    features_test_pred = get_features_test_pred(db_conn, model_id, features_table, doco_k=doco_k, joco_k=joco_k)
    print(features_test_pred)

    # Sex
    label_crosstab, pred_crosstab = create_crosstabs(features_test_pred, "dem_sex")
    print(label_crosstab)
    print(pred_crosstab)

    # Race
    label_crosstab, pred_crosstab = create_crosstabs(features_test_pred, "dem_race")
    print(label_crosstab)
    print(pred_crosstab)