from . import __root__
from os.path import join
# from utils import __root__


# ----------------------------- DIRECTORIES ----------------------------- #

PROJ_DIR = __root__
MODELS_DIR = 'models/'

# Pipeline directories
PIPELINE_DIR = 'src/pipeline'
SQL_PIPELINE_DIR = 'src/pipeline/sql'
LOGS_DIR = 'src/pipeline/logs'
LOGS_PATH = join(PROJ_DIR, LOGS_DIR)
CONFIGS_DIR = join(PROJ_DIR, PIPELINE_DIR, 'configs')
TEMPLATES_CONFIGS_DIR = join(PROJ_DIR, PIPELINE_DIR, 'config_templates')
PIPELINE_PATH = join(PROJ_DIR, PIPELINE_DIR)


# ETL directories
SEMANTICS_DIR = 'infrastructure/ETL/semantics'
SQL_SEMANTICS_DIR = 'infrastructure/ETL/semantics/sql'

# Utils directories
UTILS_DIR = 'src/utils'
SQL_UTILS_DIR = 'src/utils/sql'

# Data directories
DATA_DIR = '/mnt/data/projects/dojo-mh/'  # Edit to a disk path to save files
PREDICTIONS_DIR = join(DATA_DIR, 'predictions')
FIGURES_DIR = join(DATA_DIR, 'figures')
MODELS_PATH = join(DATA_DIR, 'models')
CSV_PATH = join(DATA_DIR, 'predictions')
MASTER_MATRIX_DIR = join(DATA_DIR, 'matrices')
MASTER_MATRIX_NAME = 'master_features.p'
MASTER_MATRIX_PATH = join(MASTER_MATRIX_DIR, MASTER_MATRIX_NAME)
SMALL_MATRICES_DIR = join(MASTER_MATRIX_DIR, 'small-mats')
DEMOGRAPHICS_DIR = join(DATA_DIR, 'demographics')
PREDICTIONS_DIR = join(DATA_DIR, 'predictions')


# ----------------------------- SQL QUERY PATHS ----------------------------- #

# Cohort
SQL_CREATE_EMPTY_COHORT_PATH = join(PROJ_DIR, SQL_PIPELINE_DIR, 'create_empty_cohort.sql')
SQL_INSERT_COHORT_PATH = join(PROJ_DIR, SQL_PIPELINE_DIR, 'insert_cohort.sql')

# Features
SQL_AGGREGATE_FEATURES = join(PROJ_DIR, SQL_PIPELINE_DIR, 'aggregation_feature.sql')
SQL_NUMERICAL_CATEGORICAL_FEATURES = join(PROJ_DIR, SQL_PIPELINE_DIR, 'numcat_feature.sql')
SQL_CREATE_FEATURES = join(PROJ_DIR, SQL_PIPELINE_DIR, 'create_features.sql')

# Labels
SQL_CREATE_LABELS_PATH = join(PROJ_DIR, SQL_PIPELINE_DIR, 'create_empty_labels.sql')
SQL_LABELS_PATH = join(PROJ_DIR, SQL_PIPELINE_DIR, 'insert_labels.sql')

# Semantic
SQL_CREATE_EMPTY_CLIENT_EVENTS_PATH = join(PROJ_DIR, SQL_SEMANTICS_DIR, 'create_empty_client_events.sql')
SQL_INSERT_CLIENT_EVENTS_PATH = join(PROJ_DIR, SQL_SEMANTICS_DIR, 'insert_client_events.sql')

SQL_CREATE_EMPTY_DEMOGRAPHICS_EVAL_PATH = join(PROJ_DIR, SQL_SEMANTICS_DIR, 'create_empty_demographics_eval.sql')
SQL_INSERT_DEMOGRAPHICS_EVAL_PATH = join(PROJ_DIR, SQL_SEMANTICS_DIR, 'insert_demographics_eval.sql')

SQL_CREATE_EMPTY_DEMOGRAPHICS_PATH = join(PROJ_DIR, SQL_SEMANTICS_DIR, 'create_empty_demographics.sql')
SQL_INSERT_DEMOGRAPHICS_PATH = join(PROJ_DIR, SQL_SEMANTICS_DIR, 'insert_demographics.sql')

# Experiments
SQL_CREATE_EXPERIMENTS_PATH = join(PROJ_DIR, SQL_PIPELINE_DIR, 'create_experiments.sql')


# ----------------------------- JSON FILES ----------------------------- #

# Table mappings
TABLE_MAPPING_PATH = join(PROJ_DIR, UTILS_DIR, 'table_mapping.json')
CLIENT_EVENTS_JSON_PATH = join(PROJ_DIR, SEMANTICS_DIR, 'client_events.json')
DEMOGRAPHICS_JSON_PATH = join(PROJ_DIR, SEMANTICS_DIR, 'demographics.json')
DEMOGRAPHICS_EVAL_JSON_PATH = join(PROJ_DIR, SEMANTICS_DIR, 'demographics_eval.json')
EVENT_DATE_JSON_PATH = join(PROJ_DIR, SEMANTICS_DIR, 'event_date.json')
TABLE_COUNTY_PATH = join(PROJ_DIR, UTILS_DIR, 'table_county_mapping.json')


# ----------------------------- CONFIG FILES ----------------------------- #

# Config file
CONFIGS_PATH = join(PROJ_DIR, PIPELINE_DIR, 'configs')

# ----------------------------- LABEL MAPPING ----------------------------- #

LABEL_MAPPING = {
    'Death only': [
        # All counties
        ['DEATH BY SUICIDE', 'DEATH BY OVERDOSE']
    ],
    'Potentially fatal': [
        # Douglas county / both counties
        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN', 'DOCO SUICIDE ATTEMPT DIAGNOSIS',
        'DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN', 'DOCO DRUG DIAGNOSIS'],

        # Johnson county
        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN',
        'DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN']
    ],
    'Suicide-related only': [
        # Douglas county / both counties
        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN', 'SUICIDAL AMBULANCE RUN',
        'DOCO SUICIDE ATTEMPT DIAGNOSIS', 'DOCO SUICIDAL DIAGNOSIS'],

        # Johnson county
        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN', 'SUICIDAL AMBULANCE RUN']
    ],
    'Drug-related only': [
        # Douglas county / both counties
        ['DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN', 'DOCO DRUG DIAGNOSIS'],

        # Johnson county
        ['DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN']
    ],
    'All behavioral crises': [
        # Douglas county / both counties
        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN', 'DOCO SUICIDE ATTEMPT DIAGNOSIS',
        'SUICIDAL AMBULANCE RUN', 'DOCO SUICIDAL DIAGNOSIS',
        'DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN', 'DOCO DRUG DIAGNOSIS',
        'OTHER BEHAVIORAL CRISIS AMBULANCE RUN', 'DOCO OTHER MENTAL CRISIS DIAGNOSIS'],

        # Johnson county
        ['DEATH BY SUICIDE', 'SUICIDE ATTEMPT AMBULANCE RUN', 'SUICIDAL AMBULANCE RUN',
        'DEATH BY OVERDOSE', 'SUBSTANCE USE AMBULANCE RUN', 'OTHER BEHAVIORAL CRISIS AMBULANCE RUN']
    ]
}
