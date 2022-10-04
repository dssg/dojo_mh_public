import pandas as pd
import numpy as np
from pipeline.baselines import FeatureRanker, LinearRanker


def test_basic_feature_ranker():
    """Ensure by-feature sorting and associated scores are computed correctly."""
    X_train = pd.DataFrame([[1, 2, 100], [2, 1, 1000], [1, 100, 2]], columns=['ambuls', 'diags', 'arrests'])
    y_train = []
    fr = FeatureRanker(['ambuls', 'diags'])
    fr.fit(X_train, y_train)
    probas = fr.predict_proba(X_train)
    correct_sort = np.argsort(probas[:, 1])[::-1].tolist() 
    assert correct_sort == [1, 2, 0]

def test_flipped_feature_ranker():
    """Ensure feature sorting flipping is performed correctly."""
    X_train = pd.DataFrame([[1, 1, 2, 100], [2, 2, 1, 1000], [3, 1, 100, 2]], columns=['days_since', 'ambuls', 'diags', 'arrests'])
    y_train = []
    fr = FeatureRanker(['__flip__days_since', 'ambuls', 'diags'])
    fr.fit(X_train, y_train)
    probas = fr.predict_proba(X_train)
    correct_sort = np.argsort(probas[:, 1])[::-1].tolist() 
    assert correct_sort == [0, 1, 2]

def test_basic_linear_ranker():
    """Ensure linear combination sorting and associated scores are computed correctly."""

    X_train = pd.DataFrame([[1, 2, 100], [1, 3, 200], [2, 4, 150]], columns=['ambuls', 'diags', 'arrests'])
    y_train = []

    # Putting equal weights on ambulance and diagnoses should result
    # in sorting of [2, 1, 0]
    weights = [0.50, 0.50]
    lr = LinearRanker(['ambuls', 'diags'], weights)
    lr.fit(X_train, y_train)
    scores = lr.predict_proba(X_train)
    correct_sort = np.argsort(scores[:, 1])[::-1].tolist() 

    assert correct_sort == [2, 1, 0]

    # Putting all weight on diagnoses should result in sorting of [2, 1, 0]
    weights = [0, 1]
    lr = LinearRanker(['ambuls', 'diags'], weights)
    lr.fit(X_train, y_train)
    scores = lr.predict_proba(X_train)
    correct_sort = np.argsort(scores[:, 1])[::-1].tolist() 

    assert correct_sort == [2, 1, 0]

    # Putting all weight on arrests should result in sorting of [1, 2, 0]
    weights = [0, 0, 1]
    lr = LinearRanker(['ambuls', 'diags', 'arrests'], weights)
    lr.fit(X_train, y_train)
    scores = lr.predict_proba(X_train)
    correct_sort = np.argsort(scores[:, 1])[::-1].tolist() 

    assert correct_sort == [1, 2, 0]

if __name__ == '__main__':
    test_flipped_feature_ranker()
