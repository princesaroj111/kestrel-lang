import logging
from typeguard import typechecked
from typing import Iterable
from datetime import timedelta

from firepit import timestamp


_logger = logging.getLogger(__name__)


@typechecked
def split_subquery_by_time_window(
    stix_pattern: str, time_win_unit_in_seconds: int
) -> Iterable[str]:
    if not time_win_unit_in_seconds:
        _logger.debug("not use time-window-based subquery")
        yield stix_pattern
    else:
        items = stix_pattern.split()
        if items[-2] != "STOP" or items[-4] != "START":
            # no timestamp in pattern
            _logger.debug("not use subquery due to no time range")
            yield stix_pattern
        else:
            stop_entire = timestamp.to_datetime(items[-1][2:-1])
            start_entire = timestamp.to_datetime(items[-3][2:-1])
            stop = stop_entire
            start = start_entire
            time_window_unit = timedelta(seconds=time_win_unit_in_seconds)
            while stop - time_window_unit > start_entire:
                start = stop - time_window_unit
                _items = items[:]
                _items[-3] = f"t'{timestamp.timefmt(start)}'"
                _items[-1] = f"t'{timestamp.timefmt(stop)}'"
                subquery_pattern = " ".join(_items)
                _logger.debug(f"subquery pattern generated: {subquery_pattern}")
                yield subquery_pattern
                stop = start
            else:
                start = start_entire
                _items = items[:]
                _items[-3] = f"t'{timestamp.timefmt(start)}'"
                _items[-1] = f"t'{timestamp.timefmt(stop)}'"
                subquery_pattern = " ".join(_items)
                _logger.debug(f"subquery pattern generated: {subquery_pattern}")
                yield subquery_pattern
