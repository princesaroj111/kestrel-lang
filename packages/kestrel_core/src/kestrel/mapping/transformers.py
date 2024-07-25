"""Kestrel Data Model Map value transformers"""

from datetime import datetime, timezone
from typing import Callable

from kestrel.exceptions import InvalidTransformerInMapping
from kestrel.mapping.path import Path
from pandas import Series

# Dict of "registered" transformers
_transformers = {}


def transformer(func: Callable) -> Callable:
    """A decorator for registering a transformer"""
    _transformers[func.__name__] = func
    return func


@transformer
def to_epoch_ms(value: str) -> int:
    """Convert a time value to milliseconds since the epoch"""
    if "." in value:
        time_pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
    else:
        time_pattern = "%Y-%m-%dT%H:%M:%SZ"
    dt = datetime.strptime(value, time_pattern).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


@transformer
def dirname(path: str) -> str:
    """Get the directory part of `path`"""
    return Path(path).dirname()


@transformer
def basename(path: str) -> str:
    """Get the filename part of `path`"""
    return Path(path).basename()


@transformer
def startswith(value: str) -> str:
    return f"{value}%"


@transformer
def winpath_startswith(value: str) -> str:
    return f"{value}\\%"


@transformer
def posixpath_startswith(value: str) -> str:
    return f"{value}/%"


@transformer
def endswith(value: str) -> str:
    return f"%{value}"


@transformer
def winpath_endswith(value: str) -> str:
    return f"%\\{value}"


@transformer
def posixpath_endswith(value: str) -> str:
    return f"%/{value}"


@transformer
def to_int(value) -> int:
    """Ensure `value` is an int"""
    try:
        return int(value)
    except ValueError:
        # Maybe it's a hexadecimal string?
        try:
            return int(value, 16)
        except:
            return -1


@transformer
def to_str(value) -> str:
    """Ensure `value` is a str"""
    return str(value)


@transformer
def lowercase(value: str) -> str:
    """Ensure `value` is all lowercase"""
    return value.lower()


@transformer
def ip_version_to_network_layer(value: int) -> str:
    if value == 4:
        return "ipv4"
    elif value == 6:
        return "ipv6"
    elif value == 99:
        return "other"
    return "unknown"


@transformer
def network_layer_to_ip_version(val: str) -> int:
    value = val.lower()
    if value == "ipv4":
        return 4
    elif value == "ipv6":
        return 6
    elif value == "other":
        return 99
    return 0


def run_transformer(transformer_name: str, value):
    """Run the registered transformer with name `transformer_name` on `value`"""
    func = _transformers.get(transformer_name)
    if func:
        result = func(value)
    else:
        raise InvalidTransformerInMapping(transformer_name)
    return result


def run_transformer_on_series(transformer_name: str, value: Series):
    """Run the registered transformer with name `transformer_name` on `value`"""
    func = _transformers.get(transformer_name)
    if func:
        result = value.apply(func)
    else:
        raise InvalidTransformerInMapping(transformer_name)
    return result
