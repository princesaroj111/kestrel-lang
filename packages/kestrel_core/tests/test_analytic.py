import logging
import os

from pandas import DataFrame

_logger = logging.getLogger(__name__)


def do_something(df: DataFrame, **kwargs):
    _logger.debug("python analytics: run pseudo-analytic")
    for k, v in kwargs.items():
        df[k] = v
    return df


def do_something_no_annotations(df):
    _logger.debug("python analytics: run pseudo-analytic with env vars")
    name = os.environ.get("name", "new_column")
    value = int(os.environ.get("value", 0))
    df[name] = value
    return df


def do_something_env(df: DataFrame):
    return do_something_no_annotations(df)
