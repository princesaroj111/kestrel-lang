from dataclasses import dataclass
from typing import List, Mapping, Union

from mashumaro.mixins.json import DataClassJSONMixin
from pandas import DataFrame


@dataclass
class NativeQuery(DataClassJSONMixin):
    # which query language
    language: str
    # what query statement
    statement: str


@dataclass
class AnalyticOperation(DataClassJSONMixin):
    # which interface
    interface: str
    # operation description
    operation: str


@dataclass
class GraphletExplanation(DataClassJSONMixin):
    # serialized IRGraph
    graph: Mapping
    # data source query
    action: Union[NativeQuery, AnalyticOperation]


@dataclass
class GraphExplanation(DataClassJSONMixin):
    graphlets: List[GraphletExplanation]


# Kestrel Display Object
Display = Union[
    str,
    dict,
    DataFrame,
    GraphExplanation,
]
