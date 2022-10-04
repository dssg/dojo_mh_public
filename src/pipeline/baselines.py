import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from collections import defaultdict


class Baseline(ABC):
    """
    I am _totally_ an sklearn.model object :^)
    """
    @abstractmethod
    def fit(self, X_train, y_train):
        pass

    @abstractmethod
    def predict_proba(self, X):
        pass


class FeatureRanker(Baseline):
    """This baseline objects ranks importance based on self.features. For example,
    if self.features=['A', 'B'] then this baseline sorts first by feature A and
    then by feature B. It will output a score of 1 for those ranked highest and
    a score of 0 for those ranked lowest. See predict_proba() for details on 
    how scores are computed. 

    NOTE: When reading feature_namees, features are ranked from greatest to least.
    If the feature's name starts with "__flip__" then this order is reversed.

    Attributes
    ----------
    features: list of features to rank on. features[0] and features[-1] are the
        most and least important features
    __name__ (str): human readable model name
    feature_importances_: vector in [0,1]^len(features) where a greater value
        indicates the greater importance of the corresponding feature

    Methods
    ----------
    fit: fill in feature_importances_ and perform sanity checks
    predict_proba: output scores
    """
    def __init__(self, features) -> None:
        self.features = features 

        # Get a mask of features with the __flip__ prefix
        self.flip_sign = [int(feat.startswith('__flip__')) for feat in self.features]
        # Remove __flip__ prefixes from features
        for i in range(len(self.features)):
            if self.flip_sign[i]:
                self.features[i] = self.features[i][len('__flip__'):]
        # Map flip_sign's 0 -> 1 and 1 -> -1
        self.flip_sign = np.array([-2*i + 1 for i in self.flip_sign])

        # Necessary sklearn variables
        self.__name__ = 'FeatureRankerBaseline'
        self.feature_importances_ = None

    def fit(self, X_train, y_train):
        """Run sanity checks and populate self.feature_importances_

        Args:
        X_train and y_train are dataframes.
        """
        # Ensure all the features requested are real features
        assert set(self.features).issubset(X_train.columns)
        assert len(self.features) == len(self.flip_sign)

        # Populate feature_importances_
        feature_names = list(X_train.columns)
        feats_dict = defaultdict(int)
        for i, feat_name in enumerate(self.features):
            feats_dict[feat_name] = len(self.features) - i
        feature_importance = np.array([feats_dict[feat_name] for feat_name in feature_names])
        feature_importance = feature_importance / np.sum(feature_importance)
        self.feature_importances_ = list(feature_importance)

    def predict_proba(self, X: pd.DataFrame):
        """Returns an n by 2 matrix. The first column is garbage and the second
        has the corresponding score.

        Args:
        X: dataframe with multiindex=(joid, as_of_date) and n rows i.e., joid-date pairs

        Returns:
        np.array of shape (n, 2) where n is the number of rows in X.
        The first column is garbage (since it is not read by modeling.py) and
        the second column is a percentile score corresponding to how high the
        joid-date ranked per self.features. The highest ranked gets a score of 1
        and lowest a score of 0. Joid-dates with the same relevant feature
        values receive the same score.

        This output has this structure to mimic the behavior of
        sklearn.model.predic_proba()
        """ 
        copy_df = pd.DataFrame(X[self.features])
        # Elementwise multiplication flips the sign of each feature when desired.
        # tuple comparison ensures we compare features with the first feature being
        # most significant
        copy_df['ranking_col'] = [tuple(self.flip_sign * row.values) for _, row in copy_df.iterrows()]
        copy_df['scores'] = copy_df['ranking_col'].rank(pct=True, method='average')

        rv = np.array([np.empty(len(copy_df.index)), copy_df['scores']]).T
        return rv


class LinearRanker(Baseline):
    """This baseline objects ranks importance based on a linear combination
    given by self.weights * self.features.
    For example, if self.weights=[0.20, 0.80] and self.features=['A', 'B'] then this
    baseline creates a score based on the linear combination and ranks people accordingly

    Attributes
    ----------
    features: list of features to rank on. features[0] and features[-1] are the
        most and least important features
    weights: list of weights to use in the linear combination
    __name__ (str): human readable model name
    feature_importances_: vector in [0,1]^len(features) where a greater value
        indicates the greater importance of the corresponding feature

    Methods
    ----------
    fit: fill in feature_importances_ and perform sanity checks
    predict_proba: output scores
    """
    def __init__(self, features, weights) -> None:
        assert len(features) == len(weights)

        self.features = features
        self.weights = np.array(weights) / sum(np.array(weights))
        self.__name__ = 'LinearRanker'
        self.feature_importances_ = None

    def fit(self, X_train, y_train):
        """Run sanity checks and populate self.feature_importances_

        Args:
        X_train and y_train are dataframes.
        """
        # Ensure all the features requested are real features
        assert set(self.features).issubset(X_train.columns)

        # Populate feature_importances_
        feature_names = list(X_train.columns)
        feats_dict = defaultdict(int)

        for i, feat_name in enumerate(self.features):
            feats_dict[feat_name] = self.weights[i]

        feature_importance = np.array([feats_dict[feat_name] for feat_name in feature_names])
        feature_importance = feature_importance / np.sum(feature_importance)
        self.feature_importances_ = list(feature_importance)

    def predict_proba(self, X: pd.DataFrame):
        """Returns an n by 2 matrix. The first col is 0 and the second one is
        the linear combination of self.weights and self.features.

        Args:
        X: dataframe with multiindex=(joid, as_of_date) and n rows i.e., joid-date pairs

        Returns:
        np.array of shape (n, 2) where n is the number of rows in X.

        This output has this structure to mimic the behavior of sklearn.model.predic_proba()
        """ 

        # Compute the score as a linear combination, then scale it to be in [0, 1]
        score = np.array((self.weights * X[self.features]).sum(axis=1))
        score = (score - score.min()) / (score.max() - score.min())

        assert min(score) == 0
        assert max(score) == 1

        rv = np.array([np.empty(len(score)), score]).T

        assert rv.shape[0] == X.shape[0] and rv.shape[1] == 2
        return rv
