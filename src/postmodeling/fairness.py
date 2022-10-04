import os
import pandas as pd
from aequitas.bias import Bias
from aequitas.group import Group
from utils.constants import DEMOGRAPHICS_DIR
from postmodeling.evaluation import get_test_pred_labels_from_csv, get_predictions


def get_demographics_data(db_conn, attributes=['sex', 'race', 'ethnicity']):
    """
    Args:
        db_conn (sqlalchemy database connection):
        attributes (list, ['sex', 'race', 'ethnicity']): attributes to get from demographics, defaults to ['sex', 'race'].

    Returns:
        dataframe of the attributes per joid
    """

    # Cashing to make running of final evaluation notebook quicker ...
    filename = 'demographics.csv'
    filedir = os.path.join(DEMOGRAPHICS_DIR, filename)

    if os.path.exists(filedir):
        print('Demographics file exists, reading from disk ...')
        return pd.read_csv(filedir)

    # Build query for attributes to audit
    # Takes the mode of the attributes we have
    # The coalesce adds 'MISSING' when the attribute is missing
    template = []
    for attribute in attributes:
            template.append(f'''
            coalesce(
                mode() within group (order by demographics_value)
                filter (where demographics_type = '{attribute}'),
                'MISSING'
            ) as {attribute}
            ''')

    attributes = ','.join(template)
    query = f'''
        select distinct
            joid,
            {attributes}
        from semantic.demographics_eval group by 1;
    '''

    # Demographics table from semantic.demographics
    df = pd.read_sql(query, db_conn)
    df.to_csv(filedir, index=False)

    return df


def get_score_attr_df(df_demographics, model_id, read_from_db=False):
    """Returns the model's prediction joined with the attributes to audit on

    Args:
        df_demographics (dataframe): dataframe with demographics data
        model_id (int): model id
        read_from_db (boolean, False): whether to read from the database (NOTE: Not implemented!)

    Returns:
        dataframe holding a model's predictions (i.e., scores) and the attribute to audit
    """

    preds = get_test_pred_labels_from_csv(model_id)
    df = pd.merge(
        preds, df_demographics, how='left', on=['joid']
    )
    df['label_value'] =  df['label'].astype(int)

    # There are many people for which we have predictions
    # But no demographics; we add a 'MISSING' for them
    df = df.fillna('MISSING')

    return df

def enrich_demographics(df, df_dem):
    """Add additional demographics data from another table (without event dates)

    Args:
        db_conn (sqlalchemy database connection)
        table_query (str): query for the sex and race of a table
        df (dataframe): dataframe which holds the predictions and some demographics
        df_dem (dataframe): dataframe which holds demographics data from semantic.demographics_eval
    Returns:
        dataframe with MISSING demographics potentially substituted by other tables
    """

    # For the missing entries above, substitute them from the Johnson county
    # demographics table (adds about ~1200 entries)
    df_dem['joid'] = df_dem['joid'].astype(int)
    df_dem.fillna('MISSING', inplace=True)

    df = df.copy()
    df = pd.merge(df, df_dem[['joid', 'sex', 'race']], how='left', on=['joid'])

    df['sex_x'].mask(df['sex_x'] == 'MISSING', df['sex_y'], inplace=True)
    df['race_x'].mask(df['race_x'] == 'MISSING', df['race_y'], inplace=True)

    # Recode ethnicity into hispanic / non-hispanic (as this is what it is)
    df['hispanic'] = df['ethnicity']
    df.loc[df['ethnicity'] == 'true', 'hispanic'] = 'YES'
    df.loc[df['ethnicity'] == 'false', 'hispanic'] = 'NO'

    df['sex'] = df['sex_x']
    df['race'] = df['race_x']
    df.drop(['sex_x', 'sex_y', 'race_x', 'race_y'], axis=1, inplace=True)
    df.fillna('MISSING', inplace=True)

    return df.drop_duplicates('joid')


def binarize_score(df, joco_k=75, doco_k=40):
    """Binarizes the score / gets the prediction (necessary for Aequitas)

    Args:
        df (dataframe): dataframe of predictions
        joco_k (int, 75)
        doco_k (int, 40)

    Returns:
        dataframe with 'score' attribute \in [0, 1]
    """
    df = get_predictions(df, joco_k=joco_k, doco_k=doco_k)

    df['score'] = df['prediction']
    return df


def get_group_metrics(df, attr_and_ref_groups, joco_k=75, doco_k=40):
    """Takes a dataframe with the attribute to audit and returns the metrics

    Args:
        df (pandas dataframe): dataframe with the attribute to audit on
        attr_and_ref_groups (dict): dictionary of the attributes and the reference groups
        joco_k (int, 75): JoCo k, defaults to 75
        doco_k (int, 40): DoCo k, defaults to 40

    Returns:
        xtab, bdf: tuple of dataframes with the metrics on
        bdf includes the attributes and reference groups, xtab does not
    """
    df = df.copy()
    df = binarize_score(df, joco_k=joco_k, doco_k=doco_k)
    g = Group()
    b = Bias()
    attr_cols = attr_and_ref_groups.keys()

    xtab, _ = g.get_crosstabs(df, attr_cols=attr_cols)
    bdf = b.get_disparity_predefined_groups(
        xtab,
        original_df=df,
        ref_groups_dict=attr_and_ref_groups
    )

    return xtab, bdf


def get_absolute_metrics(xtab):
    """Takes the dataframe of metrics and returns the absolute metrics

    Args:
        xtab (pandas dataframe): dataframe with metrics and associated things

    Returns:
        dataframe with all metrics
    """
    g = Group()
    absolute_metrics = g.list_absolute_metrics(xtab)
    return xtab[['attribute_name', 'attribute_value'] + absolute_metrics]
