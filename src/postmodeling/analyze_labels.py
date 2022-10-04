'''
Functions used in the split labels analysis notebook (and nowhere else)
'''
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from postmodeling.evaluation import get_test_pred_labels_from_csv, get_models_info


def get_predictions(db_conn, model_ids):
    """Read predictions for given models from csv files. 

    Args:
        - model_ids (list of int): list of model_ids of interest

    Returns: Pandas DataFrame with predictions.
    """
    # Get predictions for first model
    preds = get_test_pred_labels_from_csv(model_ids[0]).set_index(['joid', 'as_of_date'])
    preds = preds.reset_index()

    # Append predictions for other models 
    for model_id in model_ids[1:]:
        preds = pd.concat([preds, get_test_pred_labels_from_csv(model_id)], ignore_index=True, sort=False)

    # Set as_of_date to datetime type
    preds['as_of_date'] = pd.to_datetime(preds['as_of_date'], format='%Y-%m-%d')

    # Add label_group and type
    model_info = get_models_info(db_conn, model_ids).reset_index()
    preds = pd.merge(preds, model_info[['model_id', 'label_group', 'type']], how='outer', on=['model_id']).fillna(False)

    return preds


def get_split_labels(db_conn, label_tablename = 'split_labels'):
    """Read split labels.

    Args: 
        - label_tablename: the table to read. Either 'split_labels' or 'split_labels_all_time'
    Returns: 
        -Pandas DataFrame with split labels. 
    """
    # Get split labels
    split_labels_q = f"""
        select * from modeling.{label_tablename}
        where label;
        """
    split_labels = pd.read_sql(split_labels_q, db_conn)

    # Pivot label types into columns
    pivot_split_labels = split_labels.pivot_table(values='label', index=['joid', 'as_of_date'], columns='label_name', fill_value=False, aggfunc=np.max).reset_index()
    pivot_split_labels.columns.name = None

    # Set as_of_date to datetime type
    pivot_split_labels['as_of_date'] = pd.to_datetime(pivot_split_labels['as_of_date'], format='%Y-%m-%d')
    
    return pivot_split_labels


def get_preds_split_labels(db_conn, model_ids, joco_k=75, doco_k=40, label_tablename = 'split_labels'):
    """Get table with predictions and split labels. 

    Args:
        - model_ids (list of int): list of model_ids of interest
        - joco_k (int): top k for Johnson county
        - doco_k (int): top k for Douglas county
        - label_tablename (str): the table to read. Either 'split_labels' or 'split_labels_all_time'

    Returns: tuple of 3 Pandas DataFrames for JoCo, DoCo, and both counties.
    """
    preds = get_predictions(db_conn, model_ids)
    split_labels = get_split_labels(db_conn, label_tablename)

    df = pd.merge(preds, split_labels, how='outer', on=['joid', 'as_of_date']).fillna(False)

    joco_df = df[(df.county=='joco') & (df.county_k<=joco_k)]
    doco_df = df[(df.county=='doco') & (df.county_k<=doco_k)]
    df_top_k = df[((df.county=='joco') & (df.county_k<=joco_k)) | ((df.county=='doco') & (df.county_k<=doco_k))]

    return (joco_df, doco_df, df_top_k)


def get_label_aggregations(df):
    """Get aggregated counts of label type. 

    Args: Pandas DataFrame with predictions and split labels.

    Returns: Pandas DataFrame
    """
    df_groupby = df.groupby(['as_of_date', 'model_id', 'label_group', 'type', 'county'])[
        ['DEATH BY OVERDOSE', 'DEATH BY SUICIDE', 
        'SUBSTANCE USE AMBULANCE RUN', 'ALCOHOL RELATED AMBULANCE RUN', 'DOCO DRUG DIAGNOSIS', 'SUBSTANCE USE DIAGNOSIS',
        'SUICIDE ATTEMPT AMBULANCE RUN', 'SUICIDAL AMBULANCE RUN', 'DOCO SUICIDE ATTEMPT DIAGNOSIS', 'DOCO SUICIDAL DIAGNOSIS', 'SUICIDAL DIAGNOSIS',
        'DOCO OTHER MENTAL CRISIS DIAGNOSIS', 'OTHER BEHAVIORAL CRISIS AMBULANCE RUN', 'PSYCHOSIS DIAGNOSIS'
        ]].apply(sum)
    
    return df_groupby


def plot_split_labels(df, model_id, months_future='any', xmax=None, figsize=(18, 12)):
    """Plot number of people who have a label event. 

    Args: 
        - df (Pandas DataFrame): aggregated counts of label type
        - model_id (int): id of model to plot
        - label_period:
            A string for the number of months. If 'any': any future date, even after the period the model is tested on
            
        
    """
    df_pivoted = df.stack().unstack(level=1)
    df_pivoted = df_pivoted.reset_index()
    df_pivoted.columns.name = None
    df_pivoted= df_pivoted.set_index(['as_of_date', 'level_4', 'county'])
    df_pivoted = df_pivoted.groupby(['as_of_date', 'level_4', 'county']).apply(sum)
    df_pivoted = df_pivoted.reset_index()

    #sns.set(rc={'figure.figsize':(11.7,8.27)})
    plt.clf()
    plt.figure(figsize=figsize)
    sns.set(font_scale=2)
    sns.despine()
    sns.set_style('white')
    plt.rc("axes.spines", top=False, right=False)
    p = sns.barplot(data=df_pivoted, y='level_4', x=model_id, hue='county', orient = 'h', ci=None, estimator=sum)
    
    if months_future == 'any':
        time_period_title = f"in the future"
    else:
        time_period_title = f"in next {months_future} months"
    p.set_title("Model: " + str(model_id) + " - # People with event " + time_period_title)
    p.set(xlabel='# People', ylabel='Event')
    
    if xmax != None :
        p.set_xlim(right = xmax)
    plt.show()

    return p


def get_future_events(db_conn, table_name, joid_list, event_date, as_of_date):
    '''
    Get all events in table_name for specified joids, after the date event_date
    '''

    query = f'''
    select *
    from {table_name}
    where joid in {joid_list}
    and {event_date} > '{as_of_date}'
    '''
    future_events = pd.read_sql(query, db_conn)

    return future_events


def get_all_non_fatal_flagged_events(db_conn, joids_str, as_of_date):
    '''
    Join the ambulance and ER visits together into one dataframe
    '''

    non_fatal_flags = ['suicidal_flag', 'suicide_attempt_flag', 'drug_flag', 'alcohol_flag', 'other_mental_crisis_flag']

    ambulance_runs_false_alarms = get_future_events(db_conn, 'semantic.ambulance_runs', joids_str,'event_date', as_of_date)
    ambulance_runs_false_alarms = ambulance_runs_false_alarms[['joid', 'event_date'] +  non_fatal_flags]
    ambulance_runs_false_alarms['source'] = 'ambulance'

    ER_visits_false_alarms = get_future_events(db_conn, 'clean.joco110hsccclientmisc2eadiagnosis', joids_str, 'admission_date',as_of_date)
    ER_visits_false_alarms = ER_visits_false_alarms[['joid', 'admission_date'] +  non_fatal_flags]
    ER_visits_false_alarms = ER_visits_false_alarms.rename(columns={'admission_date':'event_date'})
    ER_visits_false_alarms['source'] = 'ER'

    all_flagged_events = pd.concat([ambulance_runs_false_alarms,ER_visits_false_alarms ], axis = 0)
    all_flagged_events.reset_index(inplace = True)

    return all_flagged_events


def get_all_flagged_events(db_conn, joid_list, as_of_date):
    
    joids_str = '(' +  ', '.join(str(s) for s in joid_list) + ')'

    all_flagged_events = get_all_non_fatal_flagged_events(db_conn, joids_str, as_of_date).reset_index(drop = True).drop(columns = 'index')
    all_flagged_events['death_flag'] = False # add a death flag (all these are automatically set to false)

    # get all the deaths, then join it with the non-death events:

    deaths_false_alarms_d = get_future_events(db_conn, 'clean.jocodcmexoverdosessuicides', joids_str, 'dateofdeath', as_of_date)
    deaths_false_alarms_j = get_future_events(db_conn, 'clean.jocojcmexoverdosessuicides', joids_str, 'dateofdeath', as_of_date)

    all_future_deaths = pd.concat([deaths_false_alarms_d, deaths_false_alarms_j], axis = 0).rename(columns= {'dateofdeath':'event_date'}).reset_index()
    all_future_deaths['source'] = 'death'
    all_future_deaths = all_future_deaths[['joid', 'event_date']]
    all_future_deaths['death_flag'] = True
    all_future_deaths

    all_flagged_events = pd.merge(all_flagged_events, all_future_deaths, how = 'outer' , on = ['joid', 'event_date', 'death_flag']).fillna(False)
    return all_flagged_events
