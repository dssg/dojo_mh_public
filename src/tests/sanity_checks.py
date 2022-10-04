'''
Includes a number of sanity checks that have been useful in the past to find / check for errors
'''
import yaml
import json
import numpy as np
import pandas as pd
from utils.constants import CONFIG_PATH, DEMOGRAPHICS_JSON_PATH, CLIENT_EVENTS_JSON_PATH
from utils.helpers import get_database_connection, to_date, get_all_table_names
from datetime import datetime, date
from pipeline import time_splitter
from postmodeling.evaluation import get_evaluation

def is_increasing(L, percent=0.80):
    return all(percent * x < y for x, y in zip(L, L[1:]))

def get_table(db_conn, table, schema='modeling'):
    query = f"""select * from {schema}.{table}"""
    return pd.read_sql(query, db_conn)

def get_date_counts(db_conn, table, schema='modeling'):
    query = f"""select as_of_date, count(*) from {schema}.{table} group by as_of_date"""
    return pd.read_sql(query, db_conn)

def get_date_county_counts(db_conn, table, schema='modeling'):
    query = f"""select county, as_of_date, count(*) from {schema}.{table} group by county, as_of_date"""
    return pd.read_sql(query, db_conn)

def get_dates(db_conn, table, schema='modeling'):
    df = get_date_counts(db_conn, table, schema=schema)
    dates = np.array(df['as_of_date'])
    dates.sort()
    return dates

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

def check_one_list_in_other(check_list, in_list): 
    '''
    Check if all items within check_list exist in in_list

    Return:
    True or False
    '''

    check_list_set = set(check_list)
    in_list_set = set(in_list)
    return check_list_set.issubset(in_list_set)


def check_which_not_included(check_list, in_list, description):
    list_not_included = ', \n'.join(a for a in list(set(check_list) - set(in_list)))

    if list_not_included != '' :
        print(f'Tables {description} are:\n{list_not_included}')



with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

def check_date(db_conn, date):
    query = f"select fix_date('{date}')"
    return pd.read_sql(query, db_conn)['fix_date'].values[0]

db_conn = get_database_connection()
folds_spec = time_splitter.get_time_split(config)
all_dates = np.array(time_splitter.get_all_dates(folds_spec))

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Sanity Check Matching
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Runs the following checks:
#     - The joids in the medical examiner data are in jocojococlient

def get_intersection(
    schema='clean',
    table_name='jocojcmexoverdosessuicides',
    source='JOCOJCMEXOVERDOSESSUICIDES.ID',
    id='id'
    ):

    sourceid = 'hash_sourceid' if 'hash' in id else 'sourceid'

    intersect_query = f'''
    select count(distinct t.joid) as counts from {schema}.{table_name} t
        inner join clean.jocojococlient c
        on t.{id} = c.{sourceid}
        and t.joid = c.joid
        and c.source = '{source}';
    '''

    table_query = f'''
    select count(distinct joid) as counts from {schema}.{table_name};
    '''

    intersect_counts = pd.read_sql(intersect_query, db_conn)['counts'][0]
    table_counts = pd.read_sql(table_query, db_conn)['counts'][0]

    return (intersect_counts, table_counts)


def test_matching_jocomedex():
    a, b = get_intersection()
    assert a == b

def test_matching_docomedex():
    a, b = get_intersection(
        schema='clean',
        table_name='jocodcmexoverdosessuicides',
        source='JOCODCMEXOVERDOSESSUICIDES.CASENUM',
        id='casenum'
    )
    assert a == b

# TODO: Check counts once table cleaned / updated
def test_matching_jocomedact():
    a, b = get_intersection(
        schema='clean',
        table_name='jocomedactincidents',
        source='JOCOMEDACTINCIDENTS.RCDID',
        id='hash_rcdid'
    )
    assert a == b

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Sanity Check Cohort 
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Runs the following checks:
#     - All dates defined in the config are in the cohort
#     - Cohort is generally increasing with time
#     - There are no dead people in the cohort (currently assumes only suicide is the label)

# All dates should be in cohort
cohort_dates = get_dates(db_conn, 'cohort')

def test_dates_cohort():
    assert np.array_equal(all_dates, cohort_dates)

# The cohort should be increasing with time
df_counts = get_date_county_counts(db_conn, 'cohort')

def test_cohort_is_increasing():
    assert is_increasing(df_counts[df_counts['county'] == 'doco']['count'])
    assert is_increasing(df_counts[df_counts['county'] == 'joco']['count'])


# There should be no dead people in the cohort
# NOTE: This only works if the label is only suicide, need to rewrite
query = """
select c.joid, c.as_of_date, label_name, label from modeling.cohort c
inner join modeling.labels
using(joid, as_of_date) where label
"""

df = pd.read_sql(query, db_conn)
groupbyed_df = df.groupby(['joid'])
first_death_as_of_dates = np.array(groupbyed_df.first()['as_of_date'])
last_death_as_of_dates = np.array(groupbyed_df.last()['as_of_date'])

# If the last as of date where they are dead is more
# than 'months_future' months away from the first as of date
# where they are dead, we have a problem
months_future = config['labels']['months_future']
months_diff = np.array([diff_month(d1, d2) for d1, d2 in zip(last_death_as_of_dates, first_death_as_of_dates)])

def test_no_dead_people_in_cohort():
    assert all(months_diff < months_future)


# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Sanity Check Labels 
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Runs the following checks:
#     - All dates defined in the config are in the labels table
#     - The counts in the cohort and label table are the same
#     - The 'Selected Labels' in the config are in fact in the labels table
#     - ...

# All dates should be in labels
label_dates = get_dates(db_conn, 'labels')

def test_all_dates_in_labels():
    assert np.array_equal(all_dates, label_dates)

# The counts should be the same in labels and cohort
label_counts = get_date_counts(db_conn, 'labels')
cohort_counts = get_date_counts(db_conn, 'cohort')

def test_equal_dates_labels_cohort():
    pd.testing.assert_frame_equal(label_counts, cohort_counts)

# 'Selected Labels' are in the labels table
sel_labels = [', '.join(config['labels']['selected_labels'])]
query = "select distinct label_name from modeling.labels;"
label_names = np.array(pd.read_sql(query, db_conn)['label_name'])

def test_selected_labels_in_table():
    assert np.array_equal(np.array(sel_labels), label_names)

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Sanity Check Features
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Runs the following checks:
#     - All ages are between 5 and 90 (as set in the config), otherwise null
#     - ...

tables = list(config['features'].keys())

# Check age, youngest and oldest set in config
youngest = 5
oldest = 90
query = f"select dem_age from features.demographics_num where dem_age not between {youngest} and {oldest}"
age = np.array(pd.read_sql(query, db_conn)["dem_age"])

# There should be nobody with age outside 5 and 90
def test_nobody_outside_age_definitions():
    assert age.size == 0


# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Sanity Check Demographics
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Runs the following checks:
#     - There is only MALE and FEMALE as sex
#     - There are no people with date of birth in the future
#     - ...

# Sex is only MALE and FEMALE
all_sexes = np.array(['MALE', 'FEMALE'])

query = "select distinct demographics_value from semantic.demographics where demographics_type = 'sex'"
sexes = np.array(pd.read_sql(query, db_conn)['demographics_value'])

def test_only_male_female_sex():
    assert np.array_equal(all_sexes, sexes)

# Check age
query = "select distinct demographics_value from semantic.demographics where demographics_type = 'dob'"
dobs = np.array(pd.read_sql(query, db_conn)['demographics_value'])
now = datetime.now().date()

# Some people are older 100 years, but this is being dealt with in the config / feature creation
# Don't do any tests here
years_diff = (now - to_date(dobs.min())).days / 365

# In feature creation we take ages between 5 and 90
# But nobody should be born in the future, as this indicates a problem with fix_date
ages = np.array([(now - to_date(x)).days / 365.25 for x in dobs])

print('There are ' + str(sum(ages < 0)) + ' people with date of birth in the future')

# However they are not in the cohort

query = f"""
select * from semantic.demographics d
inner join modeling.cohort c
on d.joid = c.joid
where demographics_type = 'dob'
and demographics_value::date > '{str(now)}'::date;
"""
dobs_cohort = np.array(pd.read_sql(query, db_conn)['demographics_value'])

def test_cohort_has_appropriate_age():
    assert dobs_cohort.size == 0


# Check that all tables with demographics are in demographics.json
# TODO: add 'ethn' to the demographics query after those columns are cleaned and added. 

def test_all_demographics_tables_included():
        
    query = '''select table_name from information_schema.columns
    where table_schema = 'raw'
    and column_name similar to '%%(sex[^ual|o]|s_e_x|gender|birth_date|birthdate|date_of_birth|dateofbirth|race)%%'
    '''

    demographics_tables_raw = list(pd.read_sql(query, db_conn)['table_name'])

    with open(DEMOGRAPHICS_JSON_PATH, 'r') as f:
        demographics_dict = json.load(f)

    # list the tables that have demographic information not included in the demographics json
    check_which_not_included(
        demographics_tables_raw, demographics_dict, description='with demographics information not included in demographics json'
    )

    assert check_one_list_in_other(demographics_tables_raw, demographics_dict), 'Not all tables with demographics information are included in demographics.json'

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Sanity Check Test Evaluation
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Runs the following checks:
#     - Tests whether the recall and precision saved on this table is the same
#       as when calculated through postmodeling.evaluation

def get_metric(df, k, metric, county='joco', col='value'):
    if county:
        df_sel = df[(df['county'] == county) & (df['county_k'] == k)]
    else:
        df_sel = df[df['k'] == k]

    return df_sel[df_sel['metric'] == metric][col].values[0]


def test_metrics_computed_correctly(model_id=1831):
    query = f'''select * from results.test_evaluations where model_id = {model_id};'''
    df = pd.read_sql(query, db_conn)
    ev = get_evaluation(db_conn, model_id)
    db_as_of_dates = df['as_of_date'].unique()

    # For all as of dates, check the equivalence across evaluation and stored table
    for as_of_date in db_as_of_dates:
        df1 = df[df['as_of_date'] == as_of_date]
        ev1 = ev[ev['as_of_date'] == as_of_date]

        joco_prec_db = get_metric(df1, 75, 'precision', 'joco')
        joco_rec_db = get_metric(df1, 75, 'recall', 'joco')

        joco_prec_ev = get_metric(ev1, 75, 'precision', 'joco', col='county_value')
        joco_rec_ev = get_metric(ev1, 75, 'recall', 'joco', col='county_value')

        assert joco_prec_db == joco_prec_ev
        assert joco_rec_db == joco_rec_ev

        doco_prec_db = get_metric(df1, 40, 'precision', 'doco')
        doco_rec_db = get_metric(df1, 40, 'recall', 'doco')

        doco_prec_ev = get_metric(ev1, 40, 'precision', 'doco', col='county_value')
        doco_rec_ev = get_metric(ev1, 40, 'recall', 'doco', col='county_value')

        assert doco_prec_db == doco_prec_ev
        assert doco_rec_db == doco_rec_ev

        prec_db = get_metric(df1, df1.k.max(), 'precision', None)
        prec_ev = get_metric(ev1, ev1.k.max(), 'precision', None)

        assert prec_db == prec_ev

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Sanity Check Tables
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

def test_all_raw_cleaned():
    # Make sure that all tables in the raw schema are in either semi clean or clean
    all_raw_tables = get_all_table_names(db_conn, 'raw')
    all_semi_clean_tables = get_all_table_names(db_conn, 'semi_clean')
    all_clean_tables = get_all_table_names(db_conn, 'clean')
    all_done_tables = all_semi_clean_tables + all_clean_tables

    # check_which_not_included(all_raw_tables, all_done_tables, description = 'in raw that have not been (semi) cleaned')
    substrings = ['kdoc', 'jocoucmodeloutput'] # tables to exclude
    raw_tables = all_raw_tables
    raw_tables = [value for value in raw_tables for subs in substrings if subs not in value]

    check_which_not_included(raw_tables, all_done_tables, description = 'in raw that have not been (semi) cleaned')

    assert check_one_list_in_other(all_raw_tables, all_done_tables), 'Not all tables in raw schema are in either semi_clean or clean schemas'

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Sanity Check Event Dates
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Runs the following checks:
#     - fix_date works on a variety of inputs

def test_fix_date():
    assert check_date(db_conn, '12/01/1990') == date(1990, 12, 1)
    assert check_date(db_conn, '12/01/90') == date(1990, 12, 1)
    assert check_date(db_conn, '12/01/2090') == None
    assert check_date(db_conn, '12/01/1890') == None


query = '''
select date_part('year', event_date) as year,
count(*)
from semantic.client_events
group by year
'''

year_counts = pd.read_sql(query, db_conn)

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Sanity Check Client Events
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Runs the following checks:
#     - Make sure all tables in clean are in client events (doesn't do it for semi clean since that's automatic)

def test_all_in_client_events():
    
    tables_to_exclude = ['jocojococlient', 'jocojcmhcdemographics_dedupe', 
        'jocojcmhcdemographics', 'mh_icd10_codes', 'jocojimsbailmstbailinfo', 
        'jocojimsbailhstbailinfo']

    all_clean_tables = get_all_table_names(db_conn, 'clean')
    all_clean_tables = list(set(all_clean_tables) - set(tables_to_exclude)) # remove tables we don't need
    
    with open(CLIENT_EVENTS_JSON_PATH, 'r') as f:
        client_events_dict = json.load(f)

    # check all tables in client events

    check_which_not_included(all_clean_tables, client_events_dict, description = 'in clean not included in client_events json')

    assert check_one_list_in_other(all_clean_tables, client_events_dict), 'Not all tables in clean are included in client_events.json'
