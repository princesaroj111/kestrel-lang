import functools
import inspect
import operator
import re
import sys
from typing import Callable

from pandas import DataFrame, Series
from typeguard import typechecked

from kestrel.ir.filter import (
    BoolExp,
    ExpOp,
    FExpression,
    FBasicComparison,
    RefComparison,
    ListOp,
    MultiComp,
    NumCompOp,
    StrCompOp,
    AbsoluteTrue,
)
from kestrel.ir.instructions import (
    Construct,
    Filter,
    Limit,
    ProjectAttrs,
    ProjectEntity,
    SourceInstruction,
    TransformingInstruction,
)
from kestrel.exceptions import (
    MismatchedFieldValueInMultiColumnComparison,
    InvalidOperatorInMultiColumnComparison,
)


@typechecked
def evaluate_source_instruction(instruction: SourceInstruction) -> DataFrame:
    eval_func = _select_eval_func(instruction.instruction)
    return eval_func(instruction)


@typechecked
def evaluate_transforming_instruction(
    instruction: TransformingInstruction, dataframe: DataFrame
) -> DataFrame:
    eval_func = _select_eval_func(instruction.instruction)
    return eval_func(instruction, dataframe)


@typechecked
def _select_eval_func(instruction_name: str) -> Callable:
    eval_funcs = inspect.getmembers(sys.modules[__name__], inspect.isfunction)
    try:
        _funcs = filter(lambda x: x[0] == "_eval_" + instruction_name, eval_funcs)
        return next(_funcs)[1]
    except StopIteration:
        raise NotImplementedError(
            f"evaluation function for {instruction_name} in dataframe cache"
        )


@typechecked
def _eval_Construct(instruction: Construct) -> DataFrame:
    return DataFrame(instruction.data)


@typechecked
def _eval_Limit(instruction: Limit, dataframe: DataFrame) -> DataFrame:
    return dataframe.head(instruction.num)


@typechecked
def _eval_ProjectAttrs(instruction: ProjectAttrs, dataframe: DataFrame) -> DataFrame:
    return dataframe[list(instruction.attrs)]


@typechecked
def _eval_ProjectEntity(instruction: ProjectEntity, dataframe: DataFrame) -> DataFrame:
    # No translation/mapping, assuming the data is already in OCSF (Kestrel extension)
    df = dataframe[[col for col in dataframe if col.startswith(instruction.ocsf_field)]]
    df.rename(columns=lambda x: x[len(instruction.ocsf_field) + 1 :], inplace=True)
    df = df.drop_duplicates()
    return df


@typechecked
def _eval_Filter(instruction: Filter, dataframe: DataFrame) -> DataFrame:
    return dataframe[_eval_Filter_exp(instruction.exp, dataframe)]


@typechecked
def _eval_Filter_exp(exp: FExpression, dataframe: DataFrame) -> Series:
    # return: a series of boolean, same length as dataframe
    if isinstance(exp, AbsoluteTrue):
        bs = Series(True, index=dataframe.index)
    elif isinstance(exp, BoolExp):
        bs = _eval_Filter_exp_BoolExp(exp, dataframe)
    elif isinstance(exp, MultiComp):
        bss = [xs for xs in _eval_Filter_exp(exp.comps, dataframe)]
        if exp.op == ExpOp.AND:
            bs = functools.reduce(lambda x, y: x & y, bss)
        elif exp.op == ExpOp.OR:
            bs = functools.reduce(lambda x, y: x | y, bss)
        else:
            raise NotImplementedError("unkown kestrel.ir.filter.ExpOp type")
    else:
        bs = _eval_Filter_exp_Comparison(exp, dataframe)
    return bs


@typechecked
def _eval_Filter_exp_BoolExp(boolexp: BoolExp, dataframe: DataFrame) -> Series:
    # return: a series of boolean, same length as dataframe
    if boolexp.op == ExpOp.AND:
        bs = _eval_Filter_exp(boolexp.lhs, dataframe) & _eval_Filter_exp(
            boolexp.rhs, dataframe
        )
    elif boolexp.op == ExpOp.OR:
        bs = _eval_Filter_exp(boolexp.lhs, dataframe) | _eval_Filter_exp(
            boolexp.rhs, dataframe
        )
    else:
        raise NotImplementedError("unkown kestrel.ir.filter.ExpOp type")
    return bs


@typechecked
def _eval_Filter_exp_Comparison(
    c: FBasicComparison,
    df: DataFrame,
) -> Series:
    # return: a series of boolean, same length as dataframe
    comp2func = {
        NumCompOp.EQ: operator.eq,
        NumCompOp.NEQ: operator.ne,
        NumCompOp.LT: operator.gt,  # value first in functools.partial
        NumCompOp.LE: operator.ge,  # value first in functools.partial
        NumCompOp.GT: operator.lt,  # value first in functools.partial
        NumCompOp.GE: operator.le,  # value first in functools.partial
        StrCompOp.EQ: operator.eq,
        StrCompOp.NEQ: operator.ne,
        StrCompOp.LIKE: lambda w, x: bool(
            re.search(w.replace(".", r"\.").replace("%", ".*?"), x)
        ),
        StrCompOp.NLIKE: lambda w, x: not bool(
            re.search(w.replace(".", r"\.").replace("%", ".*?"), x)
        ),
        StrCompOp.MATCHES: lambda w, x: bool(re.search(w, x)),
        StrCompOp.NMATCHES: lambda w, x: not bool(re.search(w, x)),
        ListOp.IN: lambda w, x: x in w,
        ListOp.NIN: lambda w, x: x not in w,
    }

    # if c.value is from previous subquery evaluation,
    # turn it into Union[List[str], List[int], List[Tuple]]
    # TODO: may upgrade from List to Set for faster IN test
    if isinstance(c.value, DataFrame):
        if len(c.value.columns) == 1:
            c.value = list(c.value.iloc[:, 0])
        else:
            c.value = list(c.value.itertuples(index=False, name=None))

    try:
        # RefComparison has .fields; others have .field
        if isinstance(c, RefComparison):
            if len(c.fields) == 1:
                bools = df[c.fields[0]].apply(
                    functools.partial(comp2func[c.op], c.value)
                )
            else:
                if not (
                    isinstance(c.value, list)
                    and isinstance(c.value[0], tuple)
                    and len(c.fields) == len(c.value[0])
                ):
                    raise MismatchedFieldValueInMultiColumnComparison(c)

                # only support ListOp.IN and ListOp.NIN
                if c.op not in (ListOp.IN, ListOp.NIN):
                    raise InvalidOperatorInMultiColumnComparison(c)

                bools = df.set_index(c.fields).index.isin(c.value)
                # keep type consistent: from ndarray to Series
                # flip boolean if the operator is "not in"
                bools = Series(bools) if c.op == ListOp.IN else ~Series(bools)
        else:
            bools = df[c.field].apply(functools.partial(comp2func[c.op], c.value))
        return bools
    except KeyError as e:
        raise e
        raise NotImplementedError(f"unkown kestrel.ir.filter.*Op type: {c.op}")
