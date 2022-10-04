from pipeline.time_splitter import get_time_split, get_all_dates
from datetime import date
from dateutil.relativedelta import relativedelta
import itertools


def test_single_train_date():
    # Train start and end dates are equal, so only have one date
    config = {
        'temporal': {
            'max_train_history_months': 60,
            'min_train_history_months': 0,
            'absolute_train_start': '2019-01-01',
            'latest_validation_date': '2019-07-01',
            'train_sampling_freq_months': 3,
            'multiple_folds': 0,
            'fold_shift_months': 1
        },
        'labels': {
            'months_future': 6
        }
    }
    all_as_of_dates = get_time_split(config)
    for train_as_of_dates, validate_as_of_date in all_as_of_dates:  # just checks 1 date since num_validation_folds = 1
        assert list(train_as_of_dates) == [date(2019, 1, 1)]
        assert validate_as_of_date == date(2019, 7, 1)


def test_multiple_train_dates():
    config = {
        'temporal': {
            'max_train_history_months': 60,
            'min_train_history_months': 0,
            'absolute_train_start': '2015-07-01',
            'latest_validation_date': '2021-01-01',
            'train_sampling_freq_months': 6,
            'multiple_folds': 0,
            'fold_shift_months': 1
        },
        'labels': {
            'months_future': 6
        }
    }
    train_start = date(2015, 7, 1)
    train_end = date(2020, 7, 1)
    param_validate_as_of_date = date(2021, 1, 1)
    train_as_of_dates, validate_as_of_date = get_time_split(config)[0]
    
    years = [year for year in reversed(range(2015, 2021))]
    months = [1, 7]  # The valid months that should show up

    # Get all valid dates in the time range and sort them from most to least recent
    target_train_dates = [date(year, month, 1) for year, month in itertools.product(years, months)]
    target_train_dates = sorted([d for d in target_train_dates if train_start <= d and d <= train_end], reverse=True)

    assert list(train_as_of_dates) == target_train_dates
    assert validate_as_of_date == param_validate_as_of_date


def test_get_all_dates():
    config = {
        'temporal': {
            'max_train_history_months': 12,
            'min_train_history_months': 0,
            'absolute_train_start': '2019-01-01',
            'latest_validation_date': '2020-01-01',
            'train_sampling_freq_months': 6,
            'multiple_folds': 1,
            'fold_shift_months': 6
        },
        'labels': {
            'months_future': 6
        }
    }

    all_as_of_dates = get_time_split(config)
    train_dates1, validation_date1 = all_as_of_dates[0]
    train_dates2, validation_date2 = all_as_of_dates[1]
    
    # Check dates are generated correctly
    assert list(train_dates1) == [date(2019, 7, 1), date(2019, 1, 1)] and validation_date1 == date(2020, 1, 1)
    assert list(train_dates2) == [date(2019, 1, 1)] and validation_date2 == date(2019, 7, 1)

    # Test date iterable flattening function
    assert get_all_dates(all_as_of_dates) == [date(2019, 1, 1), date(2019, 7, 1), date(2020, 1, 1)]


def test_max_train_history():
    config = {
        'temporal': {
            'max_train_history_months': 5,
            'min_train_history_months': 0,
            'absolute_train_start': '2019-01-01',
            'latest_validation_date': '2020-01-01',
            'train_sampling_freq_months': 6,
            'multiple_folds': 1,
            'fold_shift_months': 6
        },
        'labels': {
            'months_future': 6
        }
    }

    all_as_of_dates = get_time_split(config)
    for train_dates, _ in all_as_of_dates:
        assert len(train_dates) == 1


def test_min_train_history():
    config = {
        'temporal': {
            'max_train_history_months': 60,
            'min_train_history_months': 24,
            'absolute_train_start': '2017-01-01',
            'latest_validation_date': '2020-01-01',
            'train_sampling_freq_months': 6,
            'multiple_folds': 1,
            'fold_shift_months': 6
        },
        'labels': {
            'months_future': 6
        }
    }

    earliest_fold_as_of_dates = get_time_split(config)[-1]
    assert list(earliest_fold_as_of_dates[0]) == [date(2019, 1, 1), date(2018, 7, 1), date(2018, 1, 1), date(2017, 7, 1), date(2017, 1, 1)] 
    assert earliest_fold_as_of_dates[1] == date(2019, 7, 1)


def test_multiple_folds():
    config = {
        'temporal': {
            'max_train_history_months': 36,
            'min_train_history_months': 0,
            'absolute_train_start': '2017-01-01',
            'latest_validation_date': '2020-01-01',
            'train_sampling_freq_months': 3,
            'multiple_folds': 1,
            'fold_shift_months': 3
        },
        'labels': {
            'months_future': 6
        }
    }
    all_as_of_dates = get_time_split(config)
    i = 1
    while i < len(all_as_of_dates):
        train_dates, validation_date = all_as_of_dates[i]
        prev_train_dates, prev_validation_date = all_as_of_dates[i-1]

        # Check validation date was shifted by the correct amount
        assert prev_validation_date - relativedelta(months=config['temporal']['fold_shift_months']) == validation_date

        # Check that all elements in current list appear in previous list
        assert all(item in prev_train_dates for item in train_dates)

        i += 1


def test_dates_order():
    config = {
        'temporal': {
            'max_train_history_months': 36,
            'min_train_history_months': 0,
            'absolute_train_start': '2017-01-01',
            'latest_validation_date': '2020-01-01',
            'train_sampling_freq_months': 3,
            'multiple_folds': 1,
            'fold_shift_months': 3
        },
        'labels': {
            'months_future': 6
        }
    }
    all_as_of_dates = get_time_split(config)
    print(all_as_of_dates)
    for train_dates, _ in all_as_of_dates:
        assert list(train_dates) == sorted(train_dates, reverse=True)


def test_multiple_folds():
    config = {
        'temporal': {
            'max_train_history_months': 36,
            'min_train_history_months': 0,
            'absolute_train_start': '2017-01-01',
            'latest_validation_date': '2020-01-01',
            'train_sampling_freq_months': 3,
            'multiple_folds': 0,
            'fold_shift_months': 3
        },
        'labels': {
            'months_future': 6
        }
    }
    all_as_of_dates = get_time_split(config)
    assert len(list(all_as_of_dates)) == 1
