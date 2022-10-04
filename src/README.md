# Overview
Below we provide an overview of all the files in this part of the repository. They are abstracted into the Python packages `pipeline`, which includes all code associated with the end-to-end machine learning pipeline; `postmodeling`, which includes all code used for analyzing and exploring the models that have been run; and `utils`, which includes various helper functions used throughout. There are also a number of tests for various parts of the pipeline in `tests`.

    .
    ├── pipeline
    │    ├── config_templates
    │    │    ├── model_sets_other.yaml
    │    │    ├── model_sets_rfs.yaml
    │    │    ├── temporal_both.yaml
    │    │    ├── temporal_doco.yaml
    │    │    └── temporal_joco.yaml
    │    ├── configs
    │    ├── sql
    │    │    ├── aggregation_feature.sql
    │    │    ├── create_empty_cohort.sql
    │    │    ├── create_empty_labels.sql
    │    │    ├── create_experiments.sql
    │    │    ├── create_features.sql
    │    │    ├── create_results_schema_empty_tables.sql
    │    │    ├── insert_cohort.sql
    │    │    ├── insert_labels.sql
    │    │    └── numcat_feature.sql
    │    ├── README.md
    │    ├── __init__.py
    │    ├── baselines.py
    │    ├── cohort.py
    │    ├── features.py
    │    ├── make_configs.py
    │    ├── matrix.py
    │    ├── modeling.py
    │    └── time_splitter.py
    ├── postmodeling
    │    ├── __init__.py
    │    ├── analyze_labels.py
    │    ├── evaluation.py
    │    ├── fairness.py
    │    └── plotting.py
    ├── tests
    │    ├── sanity_checks.py
    │    ├── test_baselines.py
    │    ├── test_cleaning.py
    │    ├── test_matrix.py
    │    └── test_time_splitter.py
    ├── utils
    │    ├── __init__.py
    │    ├── constants.py
    │    ├── helpers.py
    │    ├── make_bash_runner.py
    │    ├── table_county_mapping.json
    │    └── table_mapping.json
    ├── README.md
    ├── __init__.py
    └── setup.py
