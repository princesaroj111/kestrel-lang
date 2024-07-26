from datetime import datetime

import pytest
# Use sqlite3 for testing
import sqlalchemy
from dateutil import parser
from kestrel_interface_sqlalchemy.interface import (NativeTable,
                                                    SQLAlchemyTranslator)

from kestrel.exceptions import UnsupportedOperatorError
from kestrel.ir.filter import (ExpOp, IntComparison, ListComparison, ListOp,
                               MultiComp, NumCompOp, StrComparison, StrCompOp,
                               TimeRange)
from kestrel.ir.instructions import (Filter, Limit, Offset, ProjectAttrs,
                                     ProjectEntity, Sort)

ENGINE = sqlalchemy.create_engine("sqlite:///test.db")
DIALECT = ENGINE.dialect
TABLE = "my_table"
TABLE_SCHEMA = ["CommandLine", "Image", "ProcessId", "ParentProcessId", "foo", "bar", "baz"]


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
         "SELECT DISTINCT * FROM my_table WHERE foo >= ?"),
        # Try a simple filter with sorting
        ([Filter(IntComparison('foo', NumCompOp.GE, 0)), Sort('bar')],
         "SELECT DISTINCT * FROM my_table WHERE foo >= ? ORDER BY bar DESC"),
        # Simple filter plus time range
        ([Filter(IntComparison('foo', NumCompOp.GE, 0), timerange=TimeRange(_dt('2023-12-06T08:17:00Z'), _dt('2023-12-07T08:17:00Z')))],
         "SELECT DISTINCT * FROM my_table WHERE foo >= ? AND timestamp >= ? AND timestamp < ?"),
        # Add a limit and projection
        ([Limit(3), ProjectAttrs(['foo', 'bar', 'baz']), Filter(StrComparison('foo', StrCompOp.EQ, 'abc'))],
         "SELECT DISTINCT foo, bar, baz FROM my_table WHERE foo = ? LIMIT ? OFFSET ?"),
        # Same as above but reverse order
        ([Filter(StrComparison('foo', StrCompOp.EQ, 'abc')), ProjectAttrs(['foo', 'bar', 'baz']), Limit(3)],
         "SELECT DISTINCT foo, bar, baz FROM my_table WHERE foo = ? LIMIT ? OFFSET ?"),
        ([Filter(ListComparison('foo', ListOp.NIN, ['abc', 'def']))],
         "SELECT DISTINCT * FROM my_table WHERE (foo NOT IN (__[POSTCOMPILE_foo_1]))"),
        ([Filter(StrComparison('foo', StrCompOp.MATCHES, '.*abc.*'))],
         "SELECT DISTINCT * FROM my_table WHERE foo REGEXP ?"),
        ([Filter(StrComparison('foo', StrCompOp.NMATCHES, '.*abc.*'))],
         "SELECT DISTINCT * FROM my_table WHERE foo NOT REGEXP ?"),
        ([Filter(MultiComp(ExpOp.OR, [IntComparison('foo', NumCompOp.EQ, 1), IntComparison('bar', NumCompOp.EQ, 1)]))],
         "SELECT DISTINCT * FROM my_table WHERE foo = ? OR bar = ?"),
        ([Filter(MultiComp(ExpOp.AND, [IntComparison('foo', NumCompOp.EQ, 1), IntComparison('bar', NumCompOp.EQ, 1)]))],
         "SELECT DISTINCT * FROM my_table WHERE foo = ? AND bar = ?"),
        ([Limit(1000), Offset(2000)],
         "SELECT DISTINCT * FROM my_table LIMIT ? OFFSET ?"),
        # Test entity projection
        ([Limit(3), Filter(StrComparison('process.cmd_line', StrCompOp.EQ, 'foo bar')), ProjectEntity('process', 'process')],
         "SELECT DISTINCT {} FROM my_table WHERE \"CommandLine\" = ? LIMIT ? OFFSET ?"),
    ]
)
def test_sqlalchemy_translator(iseq, sql):
    projectentities = [i for i in iseq if isinstance(i, ProjectEntity)]
    if projectentities:
        pe = projectentities[0]
        if pe.ocsf_field == "process":
            cols = '"CommandLine" AS cmd_line, "Image" AS "file.path", "ProcessId" AS pid, "ParentProcessId" AS "parent_process.pid"'
        elif pe.ocsf_field == "event":
            cols = '"CommandLine" AS "cmd_line", "Image" AS \"file.path\", "ProcessId" AS pid, "ParentProcessId" AS \"parent_process.pid\"'
        else:
            raise NotImplementedError("Type not used in tests")
    else:  # no projection; defautl to all
        cols = '*'
    trans = SQLAlchemyTranslator(NativeTable(DIALECT, TABLE, TABLE_SCHEMA, data_model_map, timefmt, "timestamp"))
    for i in iseq:
        trans.add_instruction(i)
    #result = trans.result_w_literal_binds()
    result = trans.result()
    if "{}" in sql:
        sql_stmt = sql.format(cols)
    else:
        sql_stmt = sql
    assert _remove_nl(str(result)) == sql_stmt
