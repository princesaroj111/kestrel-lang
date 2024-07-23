import collections.abc
import os
from datetime import datetime
from importlib import resources
from pathlib import Path
from pkgutil import get_data
from typing import Iterable, Mapping, Optional

from kestrel.__future__ import is_python_older_than_minor_version
from typeguard import typechecked

TIME_FMT = "%Y-%m-%dT%H:%M:%S.%f"


@typechecked
def load_data_file(package_name: str, file_name: str) -> str:
    try:
        # resources.files() is introduced in Python 3.9
        content = resources.files(package_name).joinpath(file_name).read_text()
    except AttributeError:
        # Python 3.8; deprecation warning forward
        if is_python_older_than_minor_version(9):
            content = get_data(package_name, file_name).decode("utf-8")

    return content


@typechecked
def list_folder_files(
    package_name: str,
    folder_name: str,
    prefix: Optional[str] = None,
    extension: Optional[str] = None,
) -> Iterable[str]:
    # preprocesss extension to add dot it not there
    if extension and extension[0] != ".":
        extension = "." + extension
    try:
        file_paths = resources.files(package_name).joinpath(folder_name).iterdir()
    except AttributeError:
        if is_python_older_than_minor_version(9):
            import pkg_resources

            file_names = pkg_resources.resource_listdir(package_name, folder_name)
            file_paths = [
                Path(
                    pkg_resources.resource_filename(
                        package_name, os.path.join(folder_name, filename)
                    )
                )
                for filename in file_names
            ]
    file_list = (
        f
        for f in file_paths
        if (
            f.is_file()
            and (f.name.endswith(extension) if extension else True)
            and (f.name.startswith(prefix) if prefix else True)
        )
    )
    return file_list


@typechecked
def unescape_quoted_string(s: str) -> str:
    if s.startswith("r"):
        return s[2:-1]
    else:
        return s[1:-1].encode("utf-8").decode("unicode_escape")


@typechecked
def update_nested_dict(dict_old: Mapping, dict_new: Optional[Mapping]) -> Mapping:
    if dict_new:
        for k, v in dict_new.items():
            if isinstance(v, collections.abc.Mapping) and k in dict_old:
                dict_old[k] = update_nested_dict(dict_old[k], v)
            else:
                dict_old[k] = v
    return dict_old


@typechecked
def timefmt(t: datetime, prec: int = 3) -> str:
    """Format Python datetime `t` in RFC 3339-format

    Ported from firepit.timestamp
    """
    val = t.strftime(TIME_FMT)
    parts = val.split(".")
    if len(parts) > 1:
        l = len(parts[0])
        digits = parts[1]
        num_digits = len(digits)
        if num_digits:
            l += min(num_digits, prec) + 1
    return val[:l] + "Z"
