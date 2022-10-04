import os
import numpy as np
import pandas as pd
import seaborn as sns
import aequitas.plot as ap
import matplotlib.pyplot as plt
from postmodeling.fairness import (
    get_score_attr_df,
    get_group_metrics,
    get_demographics_data,
    enrich_demographics
)
from altair_saver import save
from utils.helpers import get_label_tablename
from utils.constants import FIGURES_DIR, LABEL_MAPPING
from utils.helpers import get_database_connection, get_model_set_ids, get_model_ids
from postmodeling.evaluation import get_evaluation, get_confusion_matrix, plot_confusion_matrix


def plot_score_dist(df_score, county='joco', ylim=[0, 25], figsize=(18, 12)):
    """Plots score distribution, used in final evaluation notebook
    """
    
    df_score = df_score.copy()
    df_score['Label'] = df_score['label']

    plt.clf()
    sns.set(font_scale=1.5)
    sns.despine()
    sns.set_style('white')
    plt.figure(figsize=figsize)
    plt.rc("axes.spines", top=False, right=False)
    
    county_name = 'Johnson' if county == 'joco' else 'Douglas'
    title = county_name + ': Score distribution for best model'
    
    g = sns.FacetGrid(df_score, col='label', height=8, aspect=1)
    p = g.map(sns.distplot, 'score', hist=False, rug=True)
    
    # Add joint title
    g.fig.subplots_adjust(top=0.90)
    g.fig.suptitle(title, fontsize=20)

    p.set(xlabel='Score')

    p.set(xlim=[0, df_score.score.max()])
    if ylim:
        p.set(ylim=ylim)

    return p


def plot_pr_curve(
    df, county='joco', label_group='Potentially fatal',
    xlim=[0, 1000], ylim=[0, 0.80], legend=True, figsize=(18, 12)
):
    """Plots PR curve, used in final evaluation notebook
    """

    df = df.copy()

    plt.clf()
    sns.set(font_scale=1.5)
    sns.despine()
    sns.set_style('white')
    plt.figure(figsize=figsize)
    plt.rc("axes.spines", top=False, right=False)
    
    county_name = 'Johnson' if county == 'joco' else 'Douglas'
    title = county_name + ' county: Precision / recall for ' + label_group.lower()

    # Only look at the ks smaller than the x-axis limit
    df_plot = df[df['county_k'] <= xlim[1]].reset_index()
    
    df_plot['Metric'] = df_plot.metric.str.capitalize()
    
    p = sns.lineplot(
        data=df_plot, style='Type', x='county_k',
        y='county_value', hue='Metric', ci=None, lw=4,
        palette=['#33485E', '#CEA528']
    )

    p.set(xlabel='County k', ylabel='Value')
    plt.title(title, fontsize=28)
    
    if legend:
        plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0, ncol=1, frameon=False)

    if xlim:
        p.set(xlim=xlim)
    if ylim:
        p.set(ylim=ylim)

    return p


def plot_disparity(df_metrics, metric, attribute, fairness_threshold=None):
    """Returns disparity plot from Aequitas

    Args:
        df_metrics (dataframe): dataframe with metric
        metric (str): metric of interest
        attribute (str): attribute of interest
        fairness_threshold (int, None): fairness threshold, defaults to none

    Returns:
        disparity plot
    """
    # If missing is so small that it yields a zero, remove it
    try:
        p = ap.disparity(df_metrics, metric, attribute, fairness_threshold=fairness_threshold)
    except ZeroDivisionError:
        p = ap.disparity(df_metrics[~(df_metrics == 0).any(axis=1)], metric, attribute, fairness_threshold=fairness_threshold)
    return p


def save_all_disparity_plots(
    db_conn, experiment_id, model_type=None,
    df_dem=None, fairness_threshold=None
):
    """Creates disparity plot for a model type of an expriment

    Args:
        db_conn (sqlalchemy database connection):
        experiment_id (int, optional): experiment id
        model_type (str, None): model type (e.g., RandomForestClassifier), if None uses all in experiment
        df_dem (dataframe, None): demographics data table
        fairness_threshold (float, None): optional fairness threshold for aequitas plot
    """

    if df_dem is None:
        df_dem = get_demographics_data(db_conn, attributes=['sex', 'race'])

    attr_and_ref_groups = {'sex': 'MALE', 'race': 'W', 'county': 'joco'}

    df_model_sets = get_model_set_ids(db_conn, experiment_id)

    if model_type:
        df_model_sets = df_model_sets[df_model_sets['type'] == model_type]

    if df_model_sets.empty:
        print(model_type + ' is not in the experiment, cannot create plots.')
        return

    model_set_ids = np.array(df_model_sets['model_set_id'])

    for model_set_id in model_set_ids:
        model_ids = get_model_ids(db_conn, model_set_id)

        for model_id in model_ids:
            # Get the score and attribute dataframe
            df = get_score_attr_df(df_dem, model_id)

            # Enrich the table with demographics from other tables (that do not have an event date)
            df = enrich_demographics(db_conn, df)
            _, df_metrics = get_group_metrics(df, attr_and_ref_groups)

            for metric in ['precision', 'tpr']:
                for attr in ['sex', 'race', 'county']:
                    p = plot_disparity(df_metrics, metric, attr, fairness_threshold=fairness_threshold)

                    filename = '_'.join([
                        str(experiment_id), 'fairness', metric, attr,
                        str(model_type), str(model_set_id), str(model_id)  + '.png']
                    )
                    save(p, os.path.join(FIGURES_DIR, filename))


def plot_pr_curve_models(df, model_ids, xlim=[0, 1000], ylim=[0, 0.80], figsize=(16, 12)):
    """Visualizes the Precision / Recall curves for a model set (i.e., for each validation split)

    Args:
        df (dataframe): dataframe which holds the precision and recall
        model_ids (list): list of model ids
        xlim (list, [0, 500]):  x-axis limits
        ylim (list, None): y-axis limits
        figsize (tuple, (20, 14)): figure size, defaults to (20, 14).

    Returns:
        matplotlib figure
    """

    as_of_dates = df['as_of_date'].unique()
    nr_dates = len(as_of_dates)

    # TODO: Change hardcoded subplots
    if nr_dates == 4:
        lo, hi = (2, 2)
    if nr_dates == 5:
        lo, hi = (2, 3)
    elif nr_dates == 10:
        lo, hi = (3, 4)
        figsize = (20, 14)

    plt.clf()
    sns.set(font_scale=1)
    sns.despine()
    sns.set_style('white')
    plt.rc("axes.spines", top=False, right=False)
    fig, axs = plt.subplots(lo, hi, figsize=figsize)

    k = 0
    for i in range(lo):
        for j in range(hi):

            legend = True if (i + j) == 0 else False
            if k < nr_dates:

                ax = axs[i][j]
                title = as_of_dates[k]
                df_plot = df[df['model_id'] == model_ids[k]]

                # Only look at the ks smaller than the x-axis limit
                df_plot = pd.concat([
                    df_plot[np.logical_and(df_plot['county'] == 'joco', df_plot['county_k'] <= xlim[1])],
                    df_plot[np.logical_and(df_plot['county'] == 'doco', df_plot['county_k'] <= xlim[1])]
                ]).reset_index()

                # If the results are for both counties, plot them as such
                style = 'county' if df_plot['county'].unique().size == 2 else None

                p = sns.lineplot(
                    data=df_plot, style=style, x='county_k',
                    y='county_value', hue='metric', ax=ax, legend=legend, ci=None, lw=2
                )

                p.set(title=title, xlabel='County k', ylabel='Value')
    
                if xlim:
                    p.set(xlim=xlim)
                if ylim:
                    p.set(ylim=ylim)
                
                k = k + 1
    
    return fig


def plot_crosstabs_models(df, model_ids, doco_k=None, joco_k=None, figsize=(16, 10)):
    """Visualizes the crosstabs for a model set (i.e., for each validation split) across true labels

    Args:
        df (dataframe): dataframe which holds the precision and recall
        model_ids (list): list of model ids
        xlim (list, [0, 500]):  x-axis limits
        ylim (list, None): y-axis limits
        figsize (tuple, (20, 14)): figure size, defaults to (20, 14).

    Returns:
        matplotlib figure
    """
    as_of_dates = df['as_of_date'].unique()
    nr_dates = len(as_of_dates)

    # TODO: Change hardcoded subplots
    if nr_dates == 4:
        lo, hi = (2, 2)
    if nr_dates == 5:
        lo, hi = (2, 3)
    elif nr_dates == 10:
        lo, hi = (3, 4)
        figsize = (20, 14)

    fig, axs = plt.subplots(lo, hi, figsize=figsize)

    k = 0
    for i in range(lo):
        for j in range(hi):
            if k < nr_dates:

                ax = axs[i][j]
                title = as_of_dates[k]
                df_plot = df[df['model_id'] == model_ids[k]]

                cf = get_confusion_matrix(df_plot, doco_k=doco_k, joco_k=joco_k)
                plt_cf = plot_confusion_matrix(cf)

                ax.set_title(title)
                plt_cf.plot(ax=ax)
                
                k = k + 1
    
    return fig


def plot_feature_importance(db_conn, model_ids, nr_features=20, figsize=(18, 10), feature_names=None):
    """Visualize feature importance, averaged across validation splits

    Args:
        - db_conn (sqlalchemy database connection): database connection
        - model_ids (list): list of model ids
        - nr_features (int, 12): number of features, defaults to 20.
        - figsize (tuple, (12, 12)): figure size, defaults to (12, 12).
        - 

    Returns:
        feature importance figure
    """
    str_model_ids = ','.join([str(id) for id in model_ids])
    query = 'select * from results.feature_importance where model_id in({str_model_ids})'.format(str_model_ids=str_model_ids)

    df = pd.read_sql(query, db_conn)
    df_imp = df.groupby(['feature_name']).mean().sort_values('feature_importance', ascending=False, key=abs)
    df_plot = df_imp.head(nr_features)

    if feature_names:
        assert len(feature_names) == nr_features
        df_plot.index = feature_names
        df_plot.index.name = 'feature_name'

    plt.clf()
    plt.figure(figsize=figsize)
    sns.set(font_scale=2)
    sns.despine()
    sns.set_style('white')
    plt.rc("axes.spines", top=False, right=False)
    p = sns.barplot(
        data=df_plot.reset_index(), y='feature_name', x='feature_importance',
        color='#33485E'
    )
    p.set(xlabel='Feature importance', ylabel='')
    plt.xticks(rotation=90)
    plt.title('Top ' + str(nr_features) + ' most important features', fontsize=28)

    return p.get_figure()


def plot_models_across_time(
    db_conn, metric='precision', label_name='Potentially fatal', county='joco', show_top=None, figsize=(14, 10)
):
    """Visualize metrics across validation splits and model sets for different labels for one county

    Args:
        - db_conn (sqlalchemy database connection): database connection
        - metric (str, 'precision'): metric to plot, defaults to 'precision'
        - label_name (str): label name for one of the four label groups (see below)
        - county (str): which county to show results for ('joco' or 'doco')
        - show_top (int): plots only the top X models
        - figsize (tuple, (14, 10)): figure size, defaults to (14, 10).

    Returns:
        validation across time figure
    """

    # Get all results
    query = f'''
        select * from results.test_evaluations te 
        left join results.models
        using(model_id)
        left join results.model_sets ms 
        using(model_set_id)
        left join results.experiments e 
        using(experiment_id)
        where te.metric = '{metric}'
        and te.county = '{county}';
        --and e.ends is not null;
    '''

    # Rename last column from 'county' (from results.experiments) to 'model'
    df = pd.read_sql(query, db_conn)
    #df.columns = [*df.columns[:-1], 'Model']

    county = 'Johnson' if county == 'joco' else 'Douglas'

    if label_name:
        title = county + ': ' + label_name

    # Create custom string for various model types (model_types)
    # Create custom label name (label_groups)
    label_mapping = {
        'Potentially fatal': [ 'label_1235', 'label_12351113' ],
        'Suicide-related only': [ 'label_134', 'label_1341112' ],
        'Drug-related only': [ 'label_25', 'label_2513' ],
        'All behavioral crises': [ 'label_123456', 'label_12345611121314' ]
    }

    def get_custom_label(label):
        for key, val in label_mapping.items():
            if label in val:
                return key

    model_types = []
    label_groups = []
    for i in range(df.shape[0]):
        row = df.iloc[i]
        type = row['type']

        if type == 'LinearRanker':
            type = 'High utilizer'
        
        if type == 'FeatureRanker':
            type = '_'.join([
                'Rank',
                str(row.params.get('features')[0])
            ])

        if type == 'AdaBoostClassifier':
            type = '_'.join([
                'Ada',
                str(row.params.get('n_estimators')),
                str(row.params.get('learning_rate'))
            ])

        if type == 'GradientBoostingClassifier':
            type = '_'.join([
                'Boosting',
                str(row.params.get('n_estimators')),
                str(row.params.get('learning_rate'))
            ])

        if type == 'RandomForestClassifier':
            type = '_'.join([
                'RF',
                str(row.params.get('n_estimators')),
                str(row.params.get('max_depth'))
            ])
        
        if type == 'DecisionTreeClassifier':
            type = '_'.join([
                'DT',
                str(row.params.get('max_depth'))
            ])
        
        if type == 'LogisticRegression':
            type = '_'.join([
                'LR',
                row.params.get('penalty'),
                str(row.params.get('C'))
            ])
        
        label = '_'.join(get_label_tablename(row.config).split('_')[:2])
        model_types.append(type)
        label_groups.append(get_custom_label(label))

    df['Model type'] = model_types
    #df['Label group'] = label_groups
    df['Label group'] = df['label_group']
    df['Trained on'] = np.where(
        df.trained_on == 'both', 'Both', county
    )

    # Select particular label group (if given)
    if label_name:
        df = df[df['Label group'] == label_name]

    # Remove Decision tree, LR, and FeatureRanker for now
    df = df[~df['Model type'].str.contains('DecisionTree')]
    df = df[~df['Model type'].str.contains('FeatureRanker')]
    #df = df[~df['Model type'].str.contains('LogisticRegression')]

    # If precision, grab the relevant ones, not the last one
    if metric == 'precision':
        sel = np.where(
            np.logical_or(
                np.logical_and(df['county'] == 'joco', df['county_k'] == 75),
                np.logical_and(df['county'] == 'doco', df['county_k'] == 40)
            ), True, False
        )
        df = df[sel]
    
    # If is Douglas, need to adjust the dates to only include Douglas dates
    # For when we also use the models trained on both counties
    if county == 'Douglas':
        douglas_dates = df[df['Trained on'] == 'Douglas'].as_of_date.unique()
        sel = df.apply(lambda x: x['as_of_date'] in douglas_dates, axis=1)
        df = df[sel]

    # Sort by date and then transform to string for seaborn to work
    df['as_of_date'] = pd.to_datetime(df['as_of_date'], format='%Y-%m-%d')
    df = df.sort_values(by=['as_of_date', 'Model type'], ascending=True)
    df['as_of_date'] = df['as_of_date'].astype('string')

    # Plot only 'show_top' models (based on latest split)
    if show_top:
        latest_date = df.tail(1)['as_of_date'].values[0]
        best_model_types = df[df['as_of_date'] == latest_date].nlargest(show_top, ['value'])['Model type']
        print(best_model_types)
        index = df.index[np.in1d(df['Model type'], best_model_types)]
        df = df[df.index.isin(index)]

    plt.clf()
    plt.figure(figsize=figsize)
    sns.set(font_scale=1.5)
    sns.despine()
    sns.set_style('white')
    plt.rc("axes.spines", top=False, right=False)
    #g = sns.FacetGrid(df, col='Label group')
    #g.map(
    #   sns.lineplot,
    #   data=df, hue='Model type',
    #   x='as_of_date', y='value',
    #   palette='deep', style='County'
    #)
    n_colors = df['Model type'].unique().size
    palette = sns.color_palette('hls', n_colors=n_colors)

    if label_name:
        p = sns.lineplot(
            data=df, hue='Model type',
            x='as_of_date', y='value',
            palette=palette, style='Trained on', lw=4, ci=None
        )
    else:
        # Only plot big RandomForests across label group
        df = df[df['Model type'].str.contains('RF_10000')]
        n_colors = df['Label group'].unique().size
        palette = sns.color_palette('hls', n_colors=n_colors)

        p = sns.lineplot(
            data=df, hue='Label group',
            x='as_of_date', y='value',
            palette=palette, style='Trained on', lw=4, ci=None
        )
        title = county + ': Big Random forest across label groups'

    plt.xticks(rotation=45)
    p.set(
        ylabel=metric.capitalize(),
        xlabel='As of date'
    )
    plt.title(title, fontsize=20)
    legend = plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0, ncol=1)

    for line in legend.get_lines():
        line.set_linewidth(4)
    
    if metric == 'precision':
        p.set(ylim=[0, 0.80])
    else:
        p.set(ylim=[0, 0.20])

    return p.get_figure()


def get_filename(experiment_id, type, name, model_set_id):
    """Convenience function to create the filenames
    """
    filename =  '_'.join(
        [str(experiment_id), type, 'exp', name, str(model_set_id)]
    ) + '.png'
    
    return filename


def save_fig(fig, filepath, bbox_inches='tight', dpi=100):
    """Convenience function to save the figure, scaling the box to the figure size
    """
    fig.savefig(filepath, bbox_inches=bbox_inches, dpi=dpi)


def save_feature_importance(db_conn, model_ids, model_set_id, experiment_id, name, county='both', nr_features=10):
    """Saves the feature importance plot
    """
    fig = plot_feature_importance(db_conn, model_ids, nr_features=10)
    filename = get_filename(experiment_id, 'feature_importance_' + county, name, model_set_id)
    save_fig(fig, os.path.join(FIGURES_DIR, filename))


def save_crosstabs(df_eval, model_ids, model_set_id, experiment_id, name, doco_k=None, joco_k=75):
    """Saves the crosstab plots of predicted and actual label
    """
    fn = 'crosstabs_joco' if joco_k else 'crosstabs_doco'
    fig = plot_crosstabs_models(df_eval, model_ids, doco_k=doco_k, joco_k=joco_k)
    filename = get_filename(experiment_id, fn, name, model_set_id)
    save_fig(fig, os.path.join(FIGURES_DIR, filename))


def save_pr_curve(df_eval, model_ids, model_set_id, experiment_id, name, county='both'):
    """Saves the precision / recall plot
    """
    fig = plot_pr_curve_models(df_eval, model_ids)
    filename = get_filename(experiment_id, 'pr_curves_' + county, name, model_set_id)
    save_fig(fig, os.path.join(FIGURES_DIR, filename))


def save_models_across_time(db_conn, label_name='Potentially fatal', county='joco', metric='precision', figsize=(14, 10)):
    """Saves the model sets across validation splits
    """
    fig = plot_models_across_time(db_conn, label_name=label_name, county=county, metric=metric, figsize=figsize)

    if label_name:
        filename = '_'.join(['validation_splits', county, metric, label_name.replace(' ' ,'_').lower() + '.png'])
    else:
        filename = '_'.join(['label_group_splits', county, metric, '.png'])

    save_fig(fig, os.path.join(FIGURES_DIR, filename))


def save_all_validation_figures(db_conn, label_group_plot=False):
    """Saves all validation figures

    Args:
        db_conn (sqlalchemy database connection): database connection
    """
    counties = ['joco', 'doco']
    label_names = LABEL_MAPPING.keys()

    if label_group_plot:
        for county in counties:
            for metric in ['precision', 'recall']:
                print((county, metric))
                save_models_across_time(
                    db_conn, label_name=None, county=county, metric=metric, figsize=(14, 10)
                )
    else:
        for county in counties:
            for metric in ['precision', 'recall']:
                for ln in label_names:
                    save_models_across_time(
                        db_conn, label_name=ln, county=county, metric=metric, figsize=(14, 10)
                    )


def save_all_figures(db_conn, experiment_id):
    """For one experiment, save all relevant plots to FIGURES_DIR

    Args:
        db_conn (sqlalchemy database connection): database connection
        experiment_id (int): experiment id
    """

    config_query = f'''
        select config from results.experiments
        where experiment_id = {experiment_id};
    '''

    config = pd.read_sql(config_query, db_conn)['config'][0]

    # if config does not have this field, it was run with a previous version
    # and hence on both counties
    county = config.get('county', 'both') 

    df_model_sets = get_model_set_ids(db_conn, experiment_id)
    model_types = np.array(df_model_sets['type'])
    model_set_ids = np.array(df_model_sets['model_set_id'])

    # For each model set id
    for i in range(len(model_set_ids)):

        name = model_types[i]
        model_set_id = model_set_ids[i]
        model_ids = get_model_ids(db_conn, model_set_id)

        # Create the dataframe with precision and recall
        # This is the main bottleneck of the plotting
        df_eval = pd.concat(
            get_evaluation(db_conn, id) for id in model_ids
        )

        if county == 'both':
            save_crosstabs(df_eval, model_ids, model_set_id, experiment_id, name, doco_k=40, joco_k=None)
            save_crosstabs(df_eval, model_ids, model_set_id, experiment_id, name, doco_k=None, joco_k=75)
        
        if county == 'joco':
            save_crosstabs(df_eval, model_ids, model_set_id, experiment_id, name, doco_k=None, joco_k=75)

        if county == 'doco':
            save_crosstabs(df_eval, model_ids, model_set_id, experiment_id, name, doco_k=40, joco_k=None)

        save_pr_curve(df_eval, model_ids, model_set_id, experiment_id, name, county=county)

        save_feature_importance(
            db_conn, model_ids, model_set_id, experiment_id,
            name, county=county, nr_features=20
        )


if __name__ == '__main__':
    db_conn = get_database_connection()

    # Get big model runs
    query = '''select * from results.model_sets ms
       left join results.experiments e 
       using(experiment_id)
       where e.ends is not null
       and ms.params ->> 'n_estimators' = '10000';
    '''

    df = pd.read_sql(query, db_conn)
    expids = df['experiment_id'].tolist()
    save_all_validation_figures(db_conn, label_group_plot=True)
    
    #save_models_across_time(
    #   db_conn, label_name=None, county='joco', metric='precision', figsize=(14, 10)
    #)
    
    for expid in expids:
        try:
            save_all_figures(db_conn, expid)
        except Exception as e:
            print(e)
