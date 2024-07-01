from datetime import datetime
from dateutil import parser

from kestrel_interface_sqlalchemy.interface import SQLAlchemyTranslator
from kestrel.exceptions import UnsupportedOperatorError
from kestrel.ir.filter import (
    ExpOp,
    IntComparison,
    ListOp,
    ListComparison,
    MultiComp,
    NumCompOp,
    StrCompOp,
    StrComparison,
    TimeRange
)
from kestrel.ir.instructions import (
    Filter,
    Limit,
    Offset,
    ProjectAttrs,
    ProjectEntity,
    Sort
)

# Use sqlite3 for testing
import sqlalchemy

import pytest


ENGINE = sqlalchemy.create_engine("sqlite:///test.db")
DIALECT = ENGINE.dialect
TABLE = sqlalchemy.table("my_table")


TIMEFMT = '%Y-%m-%dT%H:%M:%S.%fZ'


def timefmt(dt: datetime):
    return f"{dt}Z"


# A much-simplified test mapping
data_model_map = {
    "process": {
        "cmd_line": "CommandLine",
        "file": {
            "path": "Image",
            # "name": [
            #     {
            #         "native_field": "Image",
            #         "native_value": "basename",
            #         "ocsf_op": "LIKE",
            #         "ocsf_value": "endswith"
            #     }
            # ]
        },
        "pid": "ProcessId",
        "parent_process": {
            "pid": "ParentProcessId",
        },
    },
}

def _dt(timestr: str) -> datetime:
    return parser.parse(timestr)


def _remove_nl(s):
    return s.replace('\n', '')


@pytest.mark.parametrize(
    "iseq, sql", [
        # Try a simple filter
        ([Filter(IntComparison('foo', NumCompOp.GE, 0))],
         "SELECT {} FROM my_table WHERE foo >= ?"),
        # Try a simple filter with sorting
        ([Filter(IntComparison('foo', NumCompOp.GE, 0)), Sort('bar')],
         "SELECT {} FROM my_table WHERE foo >= ? ORDER BY bar DESC"),
        # Simple filter plus time range
        ([Filter(IntComparison('foo', NumCompOp.GE, 0), timerange=TimeRange(_dt('2023-12-06T08:17:00Z'), _dt('2023-12-07T08:17:00Z')))],
         "SELECT {} FROM my_table WHERE foo >= ? AND timestamp >= ? AND timestamp < ?"),
        # Add a limit and projection
        ([Limit(3), ProjectAttrs(['foo', 'bar', 'baz']), Filter(StrComparison('foo', StrCompOp.EQ, 'abc'))],
         "SELECT foo AS foo, bar AS bar, baz AS baz FROM my_table WHERE foo = ? LIMIT ? OFFSET ?"),
        # Same as above but reverse order
        ([Filter(StrComparison('foo', StrCompOp.EQ, 'abc')), ProjectAttrs(['foo', 'bar', 'baz']), Limit(3)],
         "SELECT foo AS foo, bar AS bar, baz AS baz FROM my_table WHERE foo = ? LIMIT ? OFFSET ?"),
        ([Filter(ListComparison('foo', ListOp.NIN, ['abc', 'def']))],
         "SELECT {} FROM my_table WHERE (foo NOT IN (__[POSTCOMPILE_foo_1]))"),
        ([Filter(StrComparison('foo', StrCompOp.MATCHES, '.*abc.*'))],
         "SELECT {} FROM my_table WHERE foo REGEXP ?"),
        ([Filter(StrComparison('foo', StrCompOp.NMATCHES, '.*abc.*'))],
         "SELECT {} FROM my_table WHERE foo NOT REGEXP ?"),
        ([Filter(MultiComp(ExpOp.OR, [IntComparison('foo', NumCompOp.EQ, 1), IntComparison('bar', NumCompOp.EQ, 1)]))],
         "SELECT {} FROM my_table WHERE foo = ? OR bar = ?"),
        ([Filter(MultiComp(ExpOp.AND, [IntComparison('foo', NumCompOp.EQ, 1), IntComparison('bar', NumCompOp.EQ, 1)]))],
         "SELECT {} FROM my_table WHERE foo = ? AND bar = ?"),
        ([Limit(1000), Offset(2000)],
         "SELECT {} FROM my_table LIMIT ? OFFSET ?"),
        # Test entity projection
        ([Limit(3), Filter(StrComparison('cmd_line', StrCompOp.EQ, 'foo bar')), ProjectEntity('process', 'process')],
         "SELECT {} FROM my_table WHERE \"CommandLine\" = ? LIMIT ? OFFSET ?"),
    ]
)
def test_sqlalchemy_translator(iseq, sql):
    if ProjectEntity in {type(i) for i in iseq}:
        cols = '"CommandLine" AS cmd_line, "Image" AS "file.path", "ProcessId" AS pid, "ParentProcessId" AS "parent_process.pid"'
    else:
        cols = '"CommandLine" AS "process.cmd_line", "Image" AS "process.file.path", "ProcessId" AS "process.pid", "ParentProcessId" AS "process.parent_process.pid"'
    trans = SQLAlchemyTranslator(DIALECT, timefmt, "timestamp", TABLE, data_model_map)
    for i in iseq:
        trans.add_instruction(i)
    #result = trans.result_w_literal_binds()
    result = trans.result()
    assert _remove_nl(str(result)) == sql.format(cols)
