import yaml
from datetime import date
from utils.helpers import to_date
from dateutil.relativedelta import relativedelta
from utils.constants import CONFIG_PATH


def get_dates_between(date_start, date_end, months_shift, min_months, max_months):
    """ Return dates between two dates. 

    Args:
        date_start (date) - earliest date of the period
        date_end (date) - latest date of the period
        months_shift (relativedelta in months) - difference between dates
        min_months (int) -  minimum number of months between date_start and date_end
        max_months (int) - maximum number of months in time period returned (starting from date_end)
    
    Returns:
        list of train dates, None if not possible from the constraints
    """
    # Ensure minimum period between dates is satisfied
    start_end_diff = relativedelta(date_end, date_start)
    if (start_end_diff.years*12 + start_end_diff.months) < min_months:
        return None
    
    max_date_start = max(date_start, date_end - max_months)
    as_of_train_dates = []
    while date_end >= max_date_start:
        as_of_train_dates.append(date_end)
        date_end -= months_shift
    
    return as_of_train_dates if len(as_of_train_dates)>0 else None


def get_time_split(config: dict) -> tuple[tuple[tuple[date], date]]:
    """ Get as_of_dates to train and validate a model. 

    Args:
        config (dict): Takes a config file that holds the relevant parameters:
            max_train_history_months - Maximum difference between train start and train end within a fold.
            min_train_history_months - Minimum difference between train start and train end date in a given fold.
            absolute_train_start - Absolute minimum date for which it is possible to train models on (all relevant data exists).
            latest_validation_date - Maximum date for validation.
            train_sampling_freq_months - Months between train samples within one fold.
            multiple_folds - Whether to get multiple folds.
            fold_shift_months - Months between folds.
            months_future - How far the labels are in the future (e.g. death during next 6 months).

    Returns:
        tuple (list of training as of dates, validation as of date)
    """

    # Grab temporal information from the config file
    temp = config['temporal']
    max_train_history_months = relativedelta(months=temp['max_train_history_months'])
    min_train_history_months = temp['min_train_history_months']
    absolute_train_start = to_date(temp['absolute_train_start'])
    latest_validation_date = to_date(temp['latest_validation_date'])
    train_sampling_freq_months = relativedelta(months=temp['train_sampling_freq_months'])
    multiple_folds = False if temp['multiple_folds']==0 else True
    fold_shift_months = relativedelta(months=temp['fold_shift_months'])

    # How far the labels should look into the future
    months_future_int = config['labels']['months_future']
    months_future = relativedelta(months=months_future_int)

    assert latest_validation_date >= absolute_train_start

    # Generate most recent fold
    validation_date = latest_validation_date
    as_of_train_dates = get_dates_between(date_start=absolute_train_start, 
                                            date_end=validation_date - months_future, 
                                            months_shift=train_sampling_freq_months, 
                                            min_months=min_train_history_months, 
                                            max_months=max_train_history_months)
    all_as_of_dates = [(tuple(as_of_train_dates), validation_date)]
    
    if multiple_folds:
        # Generate the rest of the folds
        while True:
            validation_date -= fold_shift_months
            as_of_train_dates = get_dates_between(date_start=absolute_train_start,
                                                date_end=validation_date - months_future,
                                                months_shift=train_sampling_freq_months,
                                                min_months=min_train_history_months,
                                                max_months=max_train_history_months)
            if as_of_train_dates is None:
                 break
            all_as_of_dates.append((tuple(as_of_train_dates), validation_date))

    return tuple(all_as_of_dates)


def get_all_dates(all_as_of_dates: tuple):
    """Takes the output of get_time_splits() and returns a single sorted list
    of all as_of_dates. Useful for building the cohort.
    """
    dates = []
    for train_dates, validate_as_of_date in all_as_of_dates:
        dates += list(train_dates) + [validate_as_of_date]
    return sorted(list(set(dates)))


def get_train_and_val_dates(folds) -> list[list[date], list[date]]:
    """Takes the output of get_time_split and return 
    (all_train_dates, all_validate_dates) with sorted entries.
    """
    train_dates, validate_dates = list(map(list, zip(*folds)))
    return [sorted(list(set(dates))) for dates in [train_dates, validate_dates]]


if __name__ == '__main__':

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    time_splits = get_time_split(config)
