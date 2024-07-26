from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

from kestrel.exceptions import MismatchedFieldValueInMultiColumnComparison
from mashumaro.mixins.json import DataClassJSONMixin
from typeguard import typechecked


class NumCompOp(str, Enum):
    """Numerical comparison operators (for int and float)"""

    EQ = "="
    NEQ = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="


@dataclass
class IntComparison(DataClassJSONMixin):
    """Integer comparison expression"""

    field: str
    op: NumCompOp
    value: int


@dataclass
class FloatComparison(DataClassJSONMixin):
    """Floating point comparison expression"""

    field: str
    op: NumCompOp
    value: float


class StrCompOp(str, Enum):
    """String comparison operators"""

    EQ = "="
    NEQ = "!="
    LIKE = "LIKE"
    NLIKE = "NOT LIKE"
    MATCHES = "MATCHES"
    NMATCHES = "NOT MATCHES"


@dataclass
class StrComparison(DataClassJSONMixin):
    """String comparison expression"""

    field: str
    op: StrCompOp
    value: str


class ListOp(str, Enum):
    """List membership operator"""

    IN = "IN"
    NIN = "NOT IN"


@dataclass
class ListStrComparison(DataClassJSONMixin):
    """List of strings membership comparison expression"""

    field: str
    op: ListOp
    value: List[str]


@dataclass
class ListIntComparison(DataClassJSONMixin):
    """List of ints membership comparison expression"""

    field: str
    op: ListOp
    value: List[int]


@dataclass
class ListComparison(DataClassJSONMixin):
    """List membership comparison expression"""

    field: str
    op: ListOp
    value: Union[List[int], List[str]]


# frozen=True for generating __hash__() method
@dataclass(frozen=True)
class ReferenceValue(DataClassJSONMixin):
    """Value for reference"""

    reference: str
    attributes: Tuple[str]


@dataclass
class RefComparison(DataClassJSONMixin):
    """Referred variable comparison"""

    fields: List[str]
    op: ListOp
    value: ReferenceValue

    def __post_init__(self):
        if len(self.fields) != len(self.value.attributes):
            raise MismatchedFieldValueInMultiColumnComparison(self)


class ExpOp(str, Enum):
    """Boolean expression operator"""

    AND = "AND"
    OR = "OR"


@dataclass
class MultiComp(DataClassJSONMixin):
    """Boolean expression of multiple comparisons.

    The single operator applies to ALL comparisons, so `OR` acts like `any` and `AND` acts like `all`.
    """

    op: ExpOp
    comps: List[
        Union[
            IntComparison, FloatComparison, StrComparison, ListComparison, RefComparison
        ]
    ]


@dataclass
class BoolExp(DataClassJSONMixin):
    """Binary boolean expression of comparisons"""

    lhs: FExpression
    op: ExpOp
    rhs: FExpression


@dataclass(frozen=True)
class AbsoluteTrue(DataClassJSONMixin):
    pass


@dataclass
class TimeRange(DataClassJSONMixin):
    """The time range of interest"""

    start: Optional[datetime] = None
    stop: Optional[datetime] = None


FExpression = Union[
    IntComparison,
    FloatComparison,
    StrComparison,
    ListComparison,
    RefComparison,
    MultiComp,
    BoolExp,
    AbsoluteTrue,  # need to place at last to avoid deserliazation issue
]


FComparison = Union[
    IntComparison,
    FloatComparison,
    StrComparison,
    ListComparison,
    RefComparison,
    MultiComp,
]


FBasicComparison = Union[
    IntComparison,
    FloatComparison,
    StrComparison,
    ListComparison,
    RefComparison,
]


@typechecked
def get_references_from_exp(exp: FExpression) -> Iterable[ReferenceValue]:
    if isinstance(exp, RefComparison):
        if isinstance(exp.value, ReferenceValue):
            # if not already resolved
            yield exp.value
    elif isinstance(exp, BoolExp):
        yield from get_references_from_exp(exp.lhs)
        yield from get_references_from_exp(exp.rhs)
    elif isinstance(exp, MultiComp):
        for comp in exp.comps:
            yield from get_references_from_exp(comp)


@typechecked
def resolve_reference_with_function(
    exp: FExpression, f: Callable[[ReferenceValue], Any]
):
    if isinstance(exp, RefComparison):
        if isinstance(exp.value, ReferenceValue):
            # if not already resolved
            exp.value = f(exp.value)
    elif isinstance(exp, BoolExp):
        resolve_reference_with_function(exp.lhs, f)
        resolve_reference_with_function(exp.rhs, f)
    elif isinstance(exp, MultiComp):
        for comp in exp.comps:
            resolve_reference_with_function(comp, f)
