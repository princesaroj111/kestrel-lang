from itertools import groupby
from typing import List

from pandas import DataFrame


def variable_attributes_to_dataframe(attrs: List[str]) -> DataFrame:
    categories = []
    for k, g in groupby(sorted(attrs), lambda s: s.split(".")[0] if "." in s else ""):
        categories.append(", ".join(g))
    return DataFrame(data={"attributes": categories})
