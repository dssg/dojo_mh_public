# Pipeline
End-to-end machine learning pipeline that outputs a list of people to reach out to based on their risk of experiencing a behavioral health crisis in the next six months. The figure below gives an overview of the individual pieces of our machine learning pipeline and how they interact with the database.

<figure>
  <img src="https://github.com/dssg/dojo_mh/blob/main/content/pipeline.png" alt="Figure of the machine learning pipeline"/>
</figure>


## Configuration and Pipeline Runner
All the important parameters for a run are specified in a `config.yaml` file (stored in `/configs`), which is read by the pipeline runner which in turn runs the whole pipeline. We term one single run of the pipeline with a particular set of configurations an `experiment`. We store the experiment id (a sequential integer), the configuration parameters, and the start and end date associated with an experiment in the `results.experiments` table.

All config files are stored under `configs/`. Since we have multiple label groups and counties we could run the pipeline on, we have created a script to create all possible combinations of those, see `make_configs.py`.

Running
 
```
python make_configs.py
```

combines three config templates stored in `config_templates/`, depending on the county and desired model sets. These are `base_config.yaml`, one config specifying the model sets to be run, and a temporal config. The `/utils/make_bash_runner.py` script creates a bash script that runs all selected configs.

To run the pipeline, run the following in this directory:

```
python run.py PATH_TO_CONFIG_FILE PSQL_ROLE
```

If one wishes to recreate the cohort, label, and feature tables, run:

```
python run.py PATH_TO_CONFIG_FILE PSQL_ROLE -recreate_sources
```


## Time Splitter
The time splitter module determines the temporal validation scheme. A model is trained and validated on a given temporal `fold`, corresponding to a series of train dates and validation date (as of dates). The train period is split into multiple train as of dates - each of these dates will have features and labels associated to it. The aggregation of these features and labels corresponding to the series of train as of dates will all feed into the same training process. The validation as of date is six months after the last train as of date (although this can be changed in the config file).

The figure below illustrates the validation scheme we are using in our pipeline. A single model is trained using all sets of training features and training labels and is subsequently validated on the validation labels for that particular validation as of date. Other models from the same model set are validated on the other validation labels.

<figure>
  <img src="https://github.com/dssg/dojo_mh/blob/main/content/time_splits.png" alt="Figure of the temporal validation scheme"/>
</figure>

The relevant as of dates described above are calculated based on the following parameters:

- Maximum train history: Maximum difference between the train start and train end for a given fold (in months)
- Minimum train history: Minimum difference between train start and train end in a given fold (in months)
- Absolute train start: Earliest date for which it is possible to train models on (all relevant data is available)
- Latest validation date: Latest date for validation (allow `months_future` months into the future with existing data for validation)
- Train sampling frequency: Months between train as of dates within a given fold
- Multiple folds: Indicates whether to run multiple folds rather than a single one (restricted by other temporal contraints)
- Fold shift: Months between folds

## Cohort
For each as of date, the cohort module identifies all individuals in the data who have interacted with the system (MyRC sources) at some point throughout the previous year. It stores the person's id (joid), as of date, and county they belong to in the `modeling.cohort` table.

## Labels
For target each as of date and each person in the cohort, the labels module stores whether the predicted outcome (e.g., death by suicide or overdose) occurs in the following six months. Labels are stored in the modeling schema, under the table name `label_*`, where * indicates a different name depending on the label group under consideration.

## Features
For each target as of date and each person in the cohort, the features module creates all features specified in the configuration based on the data prior to the as of date. Features are stored in the `features` schema: categorical features in tables with the suffix `_cat`, and numerical / aggregate features in tables with the suffix `_num`.

## Matrix
Create training and validation `pandas.DataFrames` / matrices using the features
tables from the database. The first step of this process is to save to disk a
`master_df.p` pickle file. This Dataframe is essentially the features for all of the
`joids` and all of the `as_of_dates` selected by `config.yaml`'s temporal paramters.
`master_df.p` is then used to build the training and validation matrices for
each time-fold. These smaller matrices are also stored to disk. Their
categorical features are one-hot-encodings. Features values are scaled to be standard
Gaussian variables i.e., zero-mean  and standard deviation of one.

This module also creates labels though these are never saved to disk since the scope of this project includes a number of labels combinatorial in the number of relevant interests e.g., suicide-related events, drug overdose-related events, etc. In `run.py` the training and validation matrices / dataframes are joined with their corresponding labels, ensuring that each row corresponds to the same `joid` and `as_of_date` pair.

## Modeling
The modeling part of the pipeline uses the training matrices and training labels to train a particular model. The model then uses the validation matrices to create a set of predictions for the validation time period. These predictions are stored to disk and subsequently evaluated against the ground truth in terms of precision and recall at a particular `k`. This evaluation is stored in the database in the `results.test_evaluations` table. The configuration of the model sets (i.e., the model class and the hyperparameters) and the models are stored in `results.model_sets` and `results.models`, respectively.
