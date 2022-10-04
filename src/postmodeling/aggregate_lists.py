import os
import math
import pandas as pd
import numpy as np
from utils.constants import PREDICTIONS_DIR
from dateutil.relativedelta import relativedelta
from datetime import date
from postmodeling.analyze_labels import get_all_flagged_events
from pipeline.matrix import make_str_array


def df_val_date(pred_df):
    """Get the validation date for a predictions dataframe. Assumes the dataframe """
    y, m, d = pred_df.iloc[0]['as_of_date'].split('-')
    y, m, d = int(y), int(m), int(d)
    return date(y, m, d)


def get_aggregated_referral_info(db_conn, best_models, label_groups, min_val_date, max_val_date, counties=['joco', 'doco']):
    """Get dataframes with aggregated event counts for lists aggregated over the
    course of one year. Gets the outreach lists from get_referral_lists(). The
    number of events are counted up and through six months after the
    max_val_date in order to resolve labels for the last selected list.

    Args:
    -----
    best_models: doubly-nested dictionary where first layer of keys corresponds
        to the county (i.e., 'joco' and 'doco') and the second layer has key for
        'model_set_ids' with the desired model_set_ids
        that will be zipped to find predictions.
    label_groups: the label_groups corresponding to the models specified in best_models,
        in order corresponding to as the model_set_ids above.
    min_val_date: minimum validation date to make predictions for. The function
        will make outreach lists starting from min_val_date.
    max_val_date:
    """
    dfs = []
    death_dfs = []
    for county in counties:
        model_set_ids = best_models[county]['model_set_ids']
        k_high = 75 if county == 'joco' else 40
        k_low = k_high  # no need to run for multiple k at the moment
        for model_set_id, label_group in zip(model_set_ids, label_groups):
            print(f'COUNTY: {county}\t LABEL GROUP: {label_group}')
            referrals_per_k = get_referral_lists(model_set_id, k_low, k_high, county, min_val_date, max_val_date)
            # min validation date for the outreach list being generated
            list_min_val_date = min_val_date
            single_list_all_joids = get_all_referred_joids(referrals_per_k, k_high)

            # NOTE: The output contains the counts of events each selected person would have from the time they are select into a
            # list and thorugh 2022-1-1 (i.e., 2021-6-1 + 6 months)
            df_for_joid_list = None
            curr_list_death_df = None
            for i in range(len(single_list_all_joids) // k_high):
                all_joids = single_list_all_joids[i*(k_high): (i+1)*k_high]
                list_min_val_date = min_val_date + i * relativedelta(months=1)
                joid_list_all_future_events = get_all_flagged_events(db_conn, all_joids, list_min_val_date)

                event_counts = pd.DataFrame(joid_list_all_future_events.drop(labels = ['joid', 'source'], axis=1).sum(axis = 0)).rename(columns = {0: 'all_time'})

                months_future = 6
                validation_end_date = max_val_date + relativedelta(months=int(months_future))
                joid_list_all_future_events_end = get_all_flagged_events(db_conn, all_joids, validation_end_date)
                event_counts['validation_period'] = joid_list_all_future_events.drop(labels = ['joid', 'source'], axis=1).sum(axis = 0) - joid_list_all_future_events_end.drop(labels = ['joid', 'source'], axis=1).sum(axis = 0)
                event_counts['label_group'] = label_group
                event_counts['county'] = county
                event_counts = event_counts.reset_index()
                event_counts = event_counts.rename({'index': 'event_type'}, axis=1)

                if df_for_joid_list is not None:
                    df_for_joid_list += event_counts.set_index(['label_group', 'event_type', 'county'])
                else:
                    df_for_joid_list = event_counts.set_index(['label_group', 'event_type', 'county'])

                # Get suicide / od breakdown
                joids_str = make_str_array(all_joids)
                q = f"""
                select joid, suicide, overdosed, (suicide or overdosed) as suic_or_od
                from clean.jocojcmexoverdosessuicides jmex
                where joid in {joids_str}
                """
                deceased_joids = pd.read_sql(q, db_conn)
                summed_deceased_joids = deceased_joids.drop(columns=['joid']).sum(axis=0)
                # values = np.array(list(summed_deceased_joids[['suicide', 'overdosed', 'suic_or_od']].values) + [label_group, county])
                suicide_label, od_label, suic_or_od_label = list(summed_deceased_joids[['suicide', 'overdosed', 'suic_or_od']].values)
                # label_group_val, county_val = [label_group, county]
                per_list_death_df = pd.DataFrame([[suicide_label, od_label, suic_or_od_label, label_group, county]],
                        columns=['suicide', 'overdosed', 'suic_or_od', 'label_group', 'county'])
                per_list_death_df = per_list_death_df.set_index(['label_group', 'county'])

                if curr_list_death_df is not None:
                    curr_list_death_df = curr_list_death_df + per_list_death_df
                else:
                    curr_list_death_df = per_list_death_df

            death_dfs.append(curr_list_death_df)
            dfs.append(df_for_joid_list)

    dfs = [df.reset_index() for df in dfs]  # get label_group, event_type and county as columns
    concat_df = pd.concat(dfs, ignore_index=True)
    death_dfs = [df.reset_index() for df in death_dfs]  # get label_group, event_type and county as columns
    deaths_df = pd.concat(death_dfs, ignore_index=True)

    return concat_df, deaths_df


def get_referral_lists(model_set_id: int, low_k: int, high_k: int, county: str, min_val_date: date, max_val_date: date, nr_outreach_months: int = 12):
    """Get the referral lists for the models specified by the model_set_id.
    Test the range of list sizes in [low_k, high_k]. Each person can only be
    outreached once i.e., each joid will appear in at most one list.

    NOTE: Will use the latest model as_of_date that is earlier than min_val_date.
    If there is no such date, error out.
    TODO: This code can be updated to allow for each person to be outreached
    multiple times or multiple times every few months.
    TODO: Currently assumes you want to outreach over the course of one year but that can be tweaked

    Args
    ---
    model_set_id:
    low_k: smallest k to try
    high_k: largest_k to try
    county: one of joco, doco, or both
    min_val_date:
    max_val_date:
    """
    # Get the target prediction csv files
    pred_files = []
    for path in os.listdir(PREDICTIONS_DIR):
        if os.path.isfile(os.path.join(PREDICTIONS_DIR, path)):
            _, _, path_exp_id, path_model_set_id, path_model_id = path.split('_')
            path_exp_id, path_model_set_id = int(path_exp_id), int(path_model_set_id)
            # path_model_id = path_model_id.split('.')[0]  # remove csv extension
            if path_model_set_id == model_set_id:
                pred_files.append(path)

    # Get the corresponding predictions dataframes
    pred_dfs = []
    for pred_file in pred_files:
        pred_dfs.append(pd.read_csv(os.path.join(PREDICTIONS_DIR, pred_file)))

    # Sort the dataframes by validation date (i.e., as_of_date column)
    pred_dfs = sorted(pred_dfs, key=lambda my_df: df_val_date(my_df))

    df_dates = [df_val_date(df) for df in pred_dfs]
    print('all model as_of_dates: ', df_dates)
    # This latest model as_of_date that is still before min_val_date
    latest_valid_min_model_date = min_val_date
    for df in reversed(pred_dfs):
        as_of_date = df_val_date(df)
        if as_of_date <= min_val_date:
            latest_valid_min_model_date = as_of_date
            break
    if latest_valid_min_model_date != min_val_date:
        print(f'Earliest model date is {latest_valid_min_model_date} despite min_val_date of {min_val_date}.')
        min_val_date = latest_valid_min_model_date

    # Filter out dataframes with dates outside of the desired validation dates range
    new_pred_dfs = []
    for df in pred_dfs:
        df_validation_date = df_val_date(df)
        if latest_valid_min_model_date <= df_validation_date and df_validation_date <= max_val_date:
            new_pred_dfs.append(df)
    pred_dfs = new_pred_dfs
    if not pred_dfs:
        raise Exception('Empty pred_dfs. Probably no appropriate dates.')

    # Throw this error because then the first months of predictions will be made with models from the future.
    if (smallest_as_of_date_included := df_val_date(pred_dfs[0])) > min_val_date:
        print('All model_dates: ', df_dates)
        raise Exception(f'The earliest selected model as_of_date ({smallest_as_of_date_included}) should be earlier than min_val_date ({min_val_date}).')

    # Take care of using the appropriate choice of county_k / k
    county = county.lower()
    if county in ['joco', 'doco']:
        k_filter = 'county_k'
        for i in range(len(pred_dfs)):  # Filter out the undesired county
            pred_dfs[i] = pred_dfs[i][pred_dfs[i]['county'] == county]
    elif county == 'both':
        k_filter = 'k'
    else:
        raise Exception('Invalid county ' + county)

    # Sort each of the dataframes by county_k or k depending on which is appropriate
    for df in pred_dfs:
        df.sort_values(by=[k_filter], inplace=True)

    # Compute the predictions lists
    referrals_per_k = {}
    nr_months_df_spans = math.ceil(nr_outreach_months / len(pred_dfs))  # number of months a single predictions df spans (e.g., 1 or 3 months)
    nr_monthly_referrals = high_k  # partner's capacity to outreach per month
    tot_referred = 0
    for k in range(low_k, nr_monthly_referrals + 1):
        # number of people to select for referral per predictions dataframe
        nr_to_refer = nr_monthly_referrals * nr_months_df_spans
        referred = set()  # joids referred in the whole year
        referral_lists = []
        for pred_df in pred_dfs:
            period_referral_list = []  # list of referrals for the current time period
            i = 0
            while len(period_referral_list) < nr_to_refer and tot_referred < nr_outreach_months * nr_monthly_referrals:
                joid = pred_df.iloc[i]['joid']
                if joid not in referred:
                    referred.add(joid)
                    period_referral_list.append(joid)
                    tot_referred += 1
                i += 1
            referral_lists.append(period_referral_list)
        referrals_per_k[k] = referral_lists

    return referrals_per_k


def get_all_referred_joids(referrals_per_k, k):
    """Get a list of all the referred people (in the order they would have been referred).

    Args
    ---
    referrals_per_k: output of get_referral_lists()
    k: size of monthly referral lists
    """
    joids = []
    for referral_list in referrals_per_k[k]:
        joids += referral_list
    return joids
