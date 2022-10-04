import os
import sys
import yaml
import sklearn
import itertools
import sklearn.ensemble
import sklearn.neural_network
from datetime import datetime
from joblib import Parallel, delayed
from utils.constants import CONFIG_PATH, MODELS_PATH, PROJ_DIR, PIPELINE_DIR
from utils.helpers import (
    get_label_tablename,
    get_database_connection,
    insert_experiment_table_start,
    insert_experiment_table_end,
    start_logger_if_necessary
)
from pipeline import (
    cohort, features, labels,
    modeling, time_splitter, matrix
)
from pipeline.matrix import (
    load_labels,
    load_matrices,
    write_matrix_driver,
    delete_matrices_from_disk
)
from pipeline.baselines import FeatureRanker, LinearRanker

logger_now = datetime.now()  # log creation time
logger = start_logger_if_necessary(logger_now)

models_dict = {
    'sklearn.ensemble.AdaBoostClassifier': sklearn.ensemble.AdaBoostClassifier,
    'sklearn.tree.DecisionTreeClassifier': sklearn.tree.DecisionTreeClassifier,
    'sklearn.linear_model.LogisticRegression': sklearn.linear_model.LogisticRegression,
    'sklearn.ensemble.RandomForestClassifier': sklearn.ensemble.RandomForestClassifier,
    'sklearn.ensemble.GradientBoostingClassifier': sklearn.ensemble.GradientBoostingClassifier,
    'sklearn.neural_network.MLPClassifier': sklearn.neural_network.MLPClassifier,
    'FeatureRanker': FeatureRanker,
    'LinearRanker': LinearRanker
}


def run_model_set(
    county, label_tablename, grid_el,
    fold, matrices_dict, labels_dict
):
    """
    Runs a model set, which includes models for each validation split

    Args:
        county (str): which county to run the model for (joco, doco, both)
        label_tablename (str): which labels table we are using to save the predictions
        grid_el (tuple): element of the model class, params grid, and model set object
        fold (list): list of training dates and one validation date
        matrices_dict (dict): cached matrices
        labels_dict (dict): cached labels
    """
    model_class, param_dict, ms = grid_el
    db_conn = get_database_connection()
    logger = start_logger_if_necessary(logger_now)

    train_as_of_dates, validate_as_of_date = fold
    latest_train_as_of_date = max(train_as_of_dates)

    logger.info('Currently running :')
    logger.info((model_class, param_dict))
    logger.info(
        'On training / validation dates: ' +
        str(latest_train_as_of_date) + ' / ' +
        str(validate_as_of_date)
    )

    # Get the training and validation matrices and labels from the
    # cached matrices and cached labels
    X_train, X_test = matrices_dict[(fold, county)]
    y_train, y_test = labels_dict[(fold, county)]
    feature_names = list(X_train.columns)

    # Create a model for this particular fold
    m = modeling.PredictionModel(ms, str(latest_train_as_of_date))
    m.save_model(db_conn)
    logger.info('Saved model id to database')

    # Train the model
    m.train(X_train, y_train)
    logger.info('Trained model')

    # Score the model
    m.score(X_test, str(validate_as_of_date))
    logger.info('Scored model')

    # Compute and save the predictions for the validation set
    df_pred = m.save_predictions(db_conn, label_tablename)
    logger.info('Saved model predictions to database')

    # Save the evaluation for this model
    m.save_evaluations(db_conn, df_pred, k=115, joco_k=75, doco_k=40)
    logger.info('Saved model evaluations to database')
    del df_pred

    # Save the feature importances
    m.save_feature_importance(db_conn, feature_names)
    logger.info('Saved feature importance to database')

    # Save the pickled model to disk
    m.save_pickled_model(MODELS_PATH)
    logger.info('Saved pickled model to disk')


def run_pipeline(config, psql_role, create_cohort=True, create_labels=True, create_features=True):
    """Runs the pipeline, from creating the cohort to running the models and saving their output

    Args:
        config (dict): config dictionary
        create_cohort (bool): If true creates the cohort table. Defaults to True.
        create_labels (bool): If true creates the labels table. Defaults to True.
        create_features (bool): If true creates the features tables. Defaults to True.
    """

    # ------------------------------------------------------------------------
    # Setup time splitting and create cohort, labels, and features if desired
    # ------------------------------------------------------------------------
    county = config['county'] # joco, doco, or both
    logger = start_logger_if_necessary(logger_now)
    logger.info("Pipeline started, running for " + county)

    db_conn = get_database_connection()
    folds_spec = time_splitter.get_time_split(config)

    # NOTE: Cherry pick folds for testing
    # ix = [0, 3, -3, -1]
    # folds_spec = tuple(folds_spec[i] for i in ix)

    print('num folds ', len(folds_spec))

    # Get all as of dates and the label tablename,
    # depends on the county and the selected labels in the config
    as_of_dates = time_splitter.get_all_dates(folds_spec)
    label_tablename = get_label_tablename(config)

    # Create experiment table (if it does not exist already) and return the experiment id
    experiment_id = insert_experiment_table_start(db_conn, config)

    if create_cohort:
        # Create and populate the cohort
        cohort.create_empty_cohort(db_conn, psql_role)
        cohort.insert_cohort(db_conn, as_of_dates, config)
        logger.info('Created the cohort')

    if create_labels:
        # Create and populate the labels
        labels.create_empty_labels(db_conn, config, psql_role)
        labels.insert_labels(db_conn, as_of_dates, config)
        logger.info('Inserted labels')

    if create_features:
        # Create and populate the features
        features.create_features(db_conn, config, as_of_dates, psql_role)
        # Delete old master_df and train / validation matrices
        delete_matrices_from_disk()
        logger.info('Inserted features')

    # ---------------------------
    # Prepare matrices and labels
    # ---------------------------
    # Create and cache the matrices for each time fold
    logger.info("Searching for labels ...")
    label_name = ', '.join(config['labels']['selected_labels'])
    labels_dict = load_labels(db_conn, county, folds_spec, label_tablename)

    logger.info("Searching for matrices...")
    write_matrix_driver(db_conn, config)
    matrices_dict = load_matrices(county, folds_spec)

    # For each (fold, county) entry in the cached matrices
    # Convert them to pandas dataframes, which makes things easier downstream
    for key in matrices_dict.keys():
        _, county = key

        # Load the train and validation matrices
        X_train, X_val = matrices_dict[key]

        # There should be no NAs in the matrices
        assert X_train.isna().sum().sum() == 0
        assert X_val.isna().sum().sum() == 0

        # Load the train and validation labels
        y_train, y_val = labels_dict[key]

        # Join the test / validation matrices with the test / validation labels
        joined_df_train = X_train.join(y_train, how='inner')
        joined_df_val = X_val.join(y_val, how='inner')

        # Save as pandas dataframes
        matrices_dict[key] = joined_df_train.drop(columns=['label']), joined_df_val.drop(columns=['label'])
        labels_dict[key] = joined_df_train['label'], joined_df_val['label']


    # --------------------
    # Start the model runs
    # --------------------
    # Get temporal parameters from the config
    temporal_params = config['temporal']
    temporal_params['interval_back'] = config['cohort']['interval_back']
    temporal_params['months_future'] = config['labels']['months_future']

    # We save all relevant model objects / configurations into
    # model_class_params_grid, which is then distributed across cores for the model run
    models_config = config['models']
    model_class_params_grid = []

    # Create the Cartesian product of all parameters in the config for each model class
    for model_class in models_config:
        model_config = models_config[model_class]
        param_names = model_config.keys()
        param_values = model_config.values()
        param_combinations = itertools.product(*param_values)

        # For each particular parameter combination, create a ModelSet object
        for params in param_combinations:
            param_dict = dict(zip(param_names, params))

            # The combination of type of model and parameters creates a model set
            # Save this model set configuration to the database
            ms = modeling.ModelSet(models_dict[model_class], param_dict, temporal_params, experiment_id, county)
            ms.save_model_sets(db_conn)

            # Append on the relevant objects for use below
            model_class_params_grid.append((model_class, param_dict, ms))
            logger.info('Saved model set to database: ' + str(models_dict[model_class]))


    # Run all model sets and validation folds in parallel if desired
    if config['parallel']:
        Parallel(n_jobs=int(config['nr_cores']))(
         delayed(run_model_set)(
             county, label_tablename, grid_el, fold, matrices_dict, labels_dict
         ) for grid_el in model_class_params_grid for fold in folds_spec
        )

    # Otherwise run model sets and validation folds sequentially;
    # preferred for e.g. random forests which can parallelize building trees over the cores
    else:
        for grid_el in model_class_params_grid:
            for fold in folds_spec:
                run_model_set(county, label_tablename, grid_el, fold, matrices_dict, labels_dict)

    # Insert the end date to the results.experiments table
    insert_experiment_table_end(db_conn, experiment_id)
    logger.info('Pipeline was run successfully!')


if __name__ == '__main__':

    filename = sys.argv[1]
    assert '.yaml' in filename

    CONFIG_PATH = os.path.join(PROJ_DIR, PIPELINE_DIR, filename)
    psql_role = ''

    if len(sys.argv) > 2:
        psql_role = sys.argv[2]

    recreate_sources = '-recreate_sources' in sys.argv

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    start = datetime.now()
    run_pipeline(
        config,
        psql_role,
        create_cohort=recreate_sources,
        create_labels=recreate_sources,
        create_features=recreate_sources
    )
    end = datetime.now()
    logger.info('Overall pipeline run took: ' + str(end - start))
