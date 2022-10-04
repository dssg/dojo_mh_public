import os
import json
import pandas as pd
import ohio.ext.pandas
from joblib import dump
from utils.constants import CSV_PATH


class ModelSet():
    """
    A class used to represent a model set

    Attributes
    ----------
    params (dict): parameter dictionary for sklearn model
    county (str): the county on which the model set is running ('both', 'doco', or 'joco')
    temporal_params (dict): parameter dictionary for the temporal parameters
    experiment_id (int): integer indicating the experiment id
    model (sklearn model): sklearn model
    model_type (str): name of the sklearn model
    model_set_id (int): number associated with this model set

    Methods
    -------
    save_model_sets(db_conn): saves model set id, experiment id,
                              type of model, temporal and model params to results.model_sets table
    """

    def __init__(self, sklearn_model, params, temporal_params, experiment_id, county):
        self.params = params
        self.county = county
        self.temporal_params = temporal_params
        self.experiment_id = experiment_id
        self.model = sklearn_model(**self.params)
        self.model_type = sklearn_model.__name__
        self.model_set_id = None

        assert county in ['both', 'doco', 'joco']

    def save_model_sets(self, db_conn):
        query = f"""
        insert into results.model_sets (type, experiment_id, params, temporal_params)
        values ('{self.model_type}'::varchar, 
                '{self.experiment_id}'::int, 
                '{json.dumps(self.params)}'::json,
                '{json.dumps(self.temporal_params)}'::json)
        returning model_set_id
        """

        result = db_conn.execute(query)
        db_conn.execute("COMMIT")

        # Return model set id (which is a serial integer) as an attribute
        self.model_set_id = result.fetchone()[0]


class PredictionModel():
    """
    A class used to represent a single model

    Attributes
    ----------
    scores (pd.DataFrame): has index (joid, as_of_date) and single column 'score'
    recall (float): recall value
    precision (float): precision value
    k (int): value of k for precision / recall
    validation_date (str): string indicating the validation date
    train_date (str): string indicating the last train date
    params (dict): dictionary for the sklearn model parameters
    model (sklearn model): sklearn model class
    model_type (string): model type string (e.g., LogisticRegression)
    model_id (int): model id
    model_set_id (int): model set id
    experiment_id (int): integer indicating the experiment id
    county (str): the county on which the model set is trained on ('both', 'doco', or 'joco')

    Methods
    -------
    train(X_train, y_train): fits the model on the training data
    score(X, validation_date): computes the model score, sets validation date
    precision_at_k(y, k=115): computes precision at top k
    recall_at_k(y, k=115): computes recall at top k
    save_model(db_conn): inserts model set id and last train date into results.models
    save_pickled_model(path): saves model as sklearn pickle to disk
    save_predictions(db_conn): inserts model predictions into results.test_predictions
    save_evaluations(db_conn): inserts model evaluations into results.evaluations_predictions
    save_feature_importance(db_conn, feature_names): inserts feature importance scores into results.feature_importance
    """

    def __init__(self, model_set, train_date):
        self.k = None
        self.scores = None
        self.recall = None
        self.precision = None
        self.model_id = None
        self.validation_date = None
        self.train_date = train_date
        self.county = model_set.county
        self.params = model_set.params
        self.model = model_set.model
        self.model_type = model_set.model_type
        self.model_set_id = model_set.model_set_id
        self.experiment_id = model_set.experiment_id

    def train(self, X_train, y_train):
        """Trains the model

        Args:
            X_train (np.ndarray): Training matrix
            y_train (np.ndarray): Test matrix
        """
        self.model.fit(X_train, y_train)

    def score(self, X: pd.DataFrame, validation_date):
        # Get scores for label = 1
        self.validation_date = validation_date
        self.scores = pd.DataFrame(self.model.predict_proba(X)[:, 1], index=X.index, columns=['score'])
    
    def save_feature_importance(self, db_conn, feature_names):
        if self.scores is None:
            raise Exception("Score the model first!")
        
        # Saving the model creates the model_id
        # which is needed for saving the feature importance
        if self.model_id is None:
            raise Exception("Save the model first!")
        
        name = self.model_type

        # Logistic regression has coefficients as feature importance
        # Other models have the attribute model.feature_importances_
        if name == 'LogisticRegression':
            feature_importance = list(self.model.coef_[0])
        else:
            # For models that do not have a feature_importances_ attribute
            # To not write feature importances to the database
            try:
                feature_importance = list(self.model.feature_importances_)
            except:
                return
        
        feature_names = ', '.join(f"'{name}'"  for name in feature_names) 

        query = f"""
        insert into results.feature_importance (model_id, train_end_date, feature_name, feature_importance)
        values (
            {self.model_id}::int,
            '{self.train_date}'::date,
            unnest(array[{feature_names}]),
            unnest(array{feature_importance})
        )
        """

        db_conn.execute(query)
        db_conn.execute("COMMIT")
    
    def save_model(self, db_conn):
        # Save model id, model set id, and train end date to results.models table
        query = f"""
        insert into results.models (model_set_id, train_end_date)
        values ({self.model_set_id}::int, '{self.train_date}'::date)
        returning model_id
        """

        result = db_conn.execute(query)
        db_conn.execute("COMMIT")

        # Return model id (which is a serial integer) as an attribute
        self.model_id = result.fetchone()[0]
    
    def save_pickled_model(self, path):
        # Saving the model creates the model_id
        # which is needed for the the filename
        if self.model_id is None:
            raise Exception("Save the model to the database first!")
        
        filename = '_'.join(
            [self.model_type, str(self.model_set_id), str(self.model_id)]
        ) + '.joblib'

        dump(self.model, os.path.join(path, filename))

    def save_predictions(self, db_conn, label_tablename):
        """Saves the predictions to results.test_predictions
        """
        county = "'doco', 'joco'" if self.county == 'both' else "'" + self.county + "'"

        # Get labels for the specific county
        query_label = f"""
        select l.joid, co.county, l.as_of_date, label_name, label 
        from modeling.{label_tablename} l 
        left join modeling.cohort co
            on l.joid = co.joid
            and co.as_of_date = '{self.validation_date}'::date 
        where l.as_of_date = '{self.validation_date}'::date
        and co.county in({county});
        """

        # Ensures these are joined with scores on joid and date
        df = pd.read_sql(query_label, db_conn, index_col=['joid', 'as_of_date'])

        # Add score and model_id and rearrange
        # Join the scores with the df ON THE (joid, as_of_date) INDEX
        df = df.join(self.scores)  
        df['model_id'] = self.model_id

        # Get overall ks by sorting the scores
        df['k'] = df['score'].rank(ascending=False, method='first').astype(int)

        # Get county-specific ks by sorting the scores
        df['county_k'] = df.groupby('county')['score'].rank(ascending=False, method='first').astype(int)
        final_df_cols = ['model_id', 'county', 'score', 'label_name', 'label', 'k', 'county_k']

        # reset_index to get joid and as_of_dates cols
        df = df[final_df_cols].reset_index()

        # Try to save to csv
        try:
            filename = '_'.join([
                'predictions', 'exp', str(self.experiment_id), str(self.model_set_id), str(self.model_id)
            ]) + '.csv'
            filepath = os.path.join(CSV_PATH, filename)
            df.to_csv(filepath, index=False)
        except Exception as e:
            print(e)

        # NOTE: This was removed for the big model run to not clog the database.
        # Going forward we probably want to save to the database again.
        #df.pg_copy_to(
        #   name='test_predictions', con=db_conn, schema='results',
        #   index=False, if_exists='append'
        #)

        return df

    def precision_at_k(self, df_pred, k=75, county='joco'):
        """Calculates precision at k

        Args:
            df_pred (dataframe): prediction data frame
            k (int, 75): k used for precision, defaults to 75.
            county (str, 'joco'): Indicates the county, defaults to 'joco'. Use None to consider both counties.
        Returns:
            precision at top k for the county(float)
        """

        if county:
            df_sel = df_pred[(df_pred['county'] == county) & (df_pred['county_k'] <= k)]
        else:
            df_sel = df_pred[df_pred['k'] <= k]

        correct_predictions = df_sel['label'].sum() 
        precision = correct_predictions / k

        return precision

    def recall_at_k(self, df_pred, k=75, county='joco'):
        """Calculates recall at k

        Args:
            df_pred (dataframe): prediction data frame
            k (int, 75): k used for precision, defaults to 75.
            county (str, 'joco'): Indicates the county, defaults to 'joco'. Use None to consider for both counties.
        Returns:
            recall at top k for the county(float)
        """

        if county:
            df_pred = df_pred[df_pred['county'] == county]
            df_sel = df_pred[df_pred['county_k'] <= k]
        else:
            df_sel = df_pred[df_pred['k'] <= k]

        correct_predictions = df_sel['label'].sum() 
        all_true_labels = df_pred['label'].sum()

        # If there are no true labels, set recall to 0
        if all_true_labels == 0:
            return 0
        
        else:
            recall = correct_predictions / all_true_labels
            return recall

    def save_evaluations(self, db_conn, df_pred, k=115, joco_k=75, doco_k=40):
        """Saves precision and recall at specific ks to the database for easy retrieval

        Args:
            - db_conn: database connection
            - df_pred (dataframe): prediction dataframe from self.save_predictions
            - k (int, 115): total k
            - joco_k (int, 75): Johnson county k
            - doco_k (int, 40): Douglas county k
        """

        if self.county != 'both':
            k = joco_k if self.county == 'joco' else doco_k
            precision = self.precision_at_k(df_pred, k=k, county=self.county)
            recall = self.recall_at_k(df_pred, k=k, county=self.county)

            # Also store the precision at the largest k
            last_k = df_pred[df_pred['county'] == self.county].k.max()
            last_precision = self.precision_at_k(df_pred, k=last_k, county=self.county)

            county = "'" + self.county + "'"

            query = f"""
            insert into results.test_evaluations (model_id, county, as_of_date, metric, k, county_k, value)
            values
                ({self.model_id}::int, {county}, '{self.validation_date}'::date, 'precision', null, {k}, {precision}),
                ({self.model_id}::int, {county}, '{self.validation_date}'::date, 'recall', null, {k}, {recall}),
                ({self.model_id}::int, {county}, '{self.validation_date}'::date, 'precision', null, {last_k}, {last_precision})
            """

        # Save both results + total precision / recall and last precision
        else:
            joco_precision = self.precision_at_k(df_pred, k=joco_k, county='joco')
            joco_recall = self.recall_at_k(df_pred, k=joco_k, county='joco')

            doco_precision = self.precision_at_k(df_pred, k=doco_k, county='doco')
            doco_recall = self.recall_at_k(df_pred, k=doco_k, county='doco')

            total_recall = self.recall_at_k(df_pred, k=k, county=None)
            total_precision = self.precision_at_k(df_pred, k=k, county=None)

            last_k = df_pred.k.max()
            last_precision = self.precision_at_k(df_pred, k=last_k, county=None)

            query = f"""
            insert into results.test_evaluations (model_id, county, as_of_date, metric, k, county_k, value)
            values
                ({self.model_id}::int, 'joco', '{self.validation_date}'::date, 'precision', null, {joco_k}, {joco_precision}),
                ({self.model_id}::int, 'joco', '{self.validation_date}'::date, 'recall', null, {joco_k}, {joco_recall}),
                ({self.model_id}::int, 'doco', '{self.validation_date}'::date, 'precision', null, {doco_k}, {doco_precision}),
                ({self.model_id}::int, 'doco', '{self.validation_date}'::date, 'recall', null, {doco_k}, {doco_recall}),
                ({self.model_id}::int, null,   '{self.validation_date}'::date, 'precision', {k}, null, {total_precision}),
                ({self.model_id}::int, null,   '{self.validation_date}'::date, 'recall', {k}, null, {total_recall}),
                ({self.model_id}::int, null,   '{self.validation_date}'::date, 'precision', {last_k}, null, {last_precision})
            """

        db_conn.execute(query)
        db_conn.execute("COMMIT")
