import logging
from functools import reduce
from typing import Callable, Optional, List, Union

import sqlalchemy
from sqlalchemy import and_, asc, column, tuple_, desc, or_, select
from sqlalchemy.engine import Compiled, default
from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList
from sqlalchemy.sql.expression import ColumnOperators, ColumnElement, CTE
from sqlalchemy.sql.selectable import Select
from typeguard import typechecked

from kestrel.ir.filter import (
    BoolExp,
    ExpOp,
    FBasicComparison,
    RefComparison,
    ListOp,
    MultiComp,
    NumCompOp,
    StrComparison,
    StrCompOp,
    AbsoluteTrue,
)
from kestrel.ir.instructions import (
    Filter,
    Instruction,
    Limit,
    Offset,
    ProjectAttrs,
    ProjectEntity,
    Sort,
    SortDirection,
)
from kestrel.mapping.data_model import (
    translate_comparison_to_native,
    translate_projection_to_native,
)
from kestrel.exceptions import SourceSchemaNotFound

_logger = logging.getLogger(__name__)

# SQLAlchemy comparison operator functions
comp2func = {
    NumCompOp.EQ: ColumnOperators.__eq__,
    NumCompOp.NEQ: ColumnOperators.__ne__,
    NumCompOp.LT: ColumnOperators.__lt__,
    NumCompOp.LE: ColumnOperators.__le__,
    NumCompOp.GT: ColumnOperators.__gt__,
    NumCompOp.GE: ColumnOperators.__ge__,
    StrCompOp.EQ: ColumnOperators.__eq__,
    StrCompOp.NEQ: ColumnOperators.__ne__,
    StrCompOp.LIKE: ColumnOperators.like,
    StrCompOp.NLIKE: ColumnOperators.not_like,
    StrCompOp.MATCHES: ColumnOperators.regexp_match,
    StrCompOp.NMATCHES: ColumnOperators.regexp_match,  # Caller must negate
    ListOp.IN: ColumnOperators.in_,
    ListOp.NIN: ColumnOperators.not_in,
}


@typechecked
class SqlTranslator:
    def __init__(
        self,
        dialect: default.DefaultDialect,
        from_obj: Union[CTE, str],
        from_obj_schema: Optional[List[str]],  # Entity CTE does not require this
        from_obj_projection_base_field: Optional[str],
        ocsf_to_native_mapping: Optional[dict],  # CTE does not require this
        timefmt: Optional[Callable],  # CTE does not have time
        timestamp: Optional[str],  # CTE does not have time
    ):
        # Specify the schema if not Entity CTE
        # Event CTE and raw datasource need this for ProjectEntity
        self.source_schema = from_obj_schema

        # Store the mapping for translation from OCSF to native
        self.data_mapping = ocsf_to_native_mapping

        # SQLAlchemy Dialect object (e.g. from sqlalchemy.dialects import sqlite; sqlite.dialect())
        self.dialect = dialect

        # inherit projection_base_field from subquery
        self.projection_base_field = from_obj_projection_base_field

        # Time formatting function for datasource
        self.timefmt = timefmt

        # Primary timestamp field in target table
        self.timestamp = timestamp

        from_clause = (
            from_obj if isinstance(from_obj, CTE) else sqlalchemy.table(from_obj)
        )

        # SQLAlchemy statement object
        # Auto-dedup by default
        self.query: Select = select("*").select_from(from_clause).distinct()

    @typechecked
    def _render_comp(self, comp: FBasicComparison) -> BinaryExpression:
        if isinstance(comp, RefComparison):  # no translation for subquery
            # most FBasicComparison has .field; RefComparison has .fields
            # col: ColumnElement
            if len(comp.fields) == 1:
                col = column(comp.fields[0])
            else:
                col = tuple_(*[column(field) for field in comp.fields])
            rendered_comp = comp2func[comp.op](col, comp.value)
        elif self.data_mapping:  # translation needed
            comps = translate_comparison_to_native(
                self.data_mapping, comp.field, comp.op, comp.value
            )
            translated_comps = (
                (
                    ~comp2func[op](column(field), value)
                    if op == StrCompOp.NMATCHES
                    else comp2func[op](column(field), value)
                )
                for field, op, value in comps
            )
            rendered_comp = reduce(or_, translated_comps)
        else:  # no translation
            rendered_comp = (
                ~comp2func[comp.op](column(comp.field), comp.value)
                if comp.op == StrCompOp.NMATCHES
                else comp2func[comp.op](column(comp.field), comp.value)
            )

        return rendered_comp

    @typechecked
    def _render_multi_comp(self, comps: MultiComp) -> BooleanClauseList:
        op = and_ if comps.op == ExpOp.AND else or_
        return reduce(op, map(self._render_comp, comps.comps))

    @typechecked
    def _render_true(self) -> ColumnElement:
        return sqlalchemy.true()

    @typechecked
    def _render_exp(self, exp: BoolExp) -> ColumnElement:
        if isinstance(exp.lhs, AbsoluteTrue):
            lhs = self._render_true()
        elif isinstance(exp.lhs, BoolExp):
            lhs = self._render_exp(exp.lhs)
        elif isinstance(exp.lhs, MultiComp):
            lhs = self._render_multi_comp(exp.lhs)
        else:
            lhs = self._render_comp(exp.lhs)

        if isinstance(exp.rhs, AbsoluteTrue):
            rhs = self._render_true()
        elif isinstance(exp.rhs, BoolExp):
            rhs = self._render_exp(exp.rhs)
        elif isinstance(exp.rhs, MultiComp):
            rhs = self._render_multi_comp(exp.rhs)
        else:
            rhs = self._render_comp(exp.rhs)

        return and_(lhs, rhs) if exp.op == ExpOp.AND else or_(lhs, rhs)

    @typechecked
    def filter_to_selection(self, filt: Filter) -> ColumnElement:
        if filt.timerange.start:
            # Convert the timerange to the appropriate pair of comparisons
            start_comp = StrComparison(
                self.timestamp, ">=", self.timefmt(filt.timerange.start)
            )
            stop_comp = StrComparison(
                self.timestamp, "<", self.timefmt(filt.timerange.stop)
            )
            # AND them together
            time_exp = BoolExp(start_comp, ExpOp.AND, stop_comp)
            # AND that with any existing filter expression
            exp = BoolExp(filt.exp, ExpOp.AND, time_exp)
        else:
            exp = filt.exp
        if isinstance(exp, AbsoluteTrue):
            selection = self._render_true()
        elif isinstance(exp, BoolExp):
            selection = self._render_exp(exp)
        elif isinstance(exp, MultiComp):
            selection = self._render_multi_comp(exp)
        else:
            selection = self._render_comp(exp)
        return selection

    def add_Filter(self, filt: Filter) -> None:
        selection = self.filter_to_selection(filt)
        self.query = self.query.where(selection)

    def add_ProjectAttrs(self, proj: ProjectAttrs) -> None:
        cols = [column(col) for col in proj.attrs]
        self.query = self.query.with_only_columns(*cols)

    def add_ProjectEntity(self, proj: ProjectEntity) -> None:
        # TODO: Project Event

        if self.projection_base_field:
            raise NotImplementedError("Dual Entity Projection In Path")

        self.projection_base_field = proj.ocsf_field

        # this will only be called to project from events
        if not self.source_schema:
            raise SourceSchemaNotFound(self.result_w_literal_binds())

        if self.data_mapping:
            pairs = translate_projection_to_native(
                self.data_mapping, proj.ocsf_field, None, self.source_schema
            )
        else:
            prefix = proj.ocsf_field + "."
            pairs = [
                (col, col[len(prefix) :])
                for col in self.source_schema
                if col.startswith(prefix)
            ]

        _logger.debug(f"column projection pairs: {pairs}")
        cols = [sqlalchemy.column(i).label(j) for i, j in pairs]
        self.query = self.query.with_only_columns(*cols)

    def add_Limit(self, lim: Limit) -> None:
        self.query = self.query.limit(lim.num)

    def add_Offset(self, offset: Offset) -> None:
        self.query = self.query.offset(offset.num)

    def add_Sort(self, sort: Sort) -> None:
        col = column(sort.attribute)
        order = asc(col) if sort.direction == SortDirection.ASC else desc(col)
        self.query = self.query.order_by(order)

    def add_instruction(self, i: Instruction) -> None:
        inst_name = i.instruction
        method_name = f"add_{inst_name}"
        method = getattr(self, method_name)
        if not method:
            raise NotImplementedError(f"SqlTranslator.{method_name}")
        method(i)

    def result(self) -> Compiled:
        return self.query.compile(dialect=self.dialect)

    def result_w_literal_binds(self) -> Compiled:
        # full SQL query with literal binds showing, i.e., IN [99, 51], not IN [?, ?]
        # this is for debug display, not used by an sqlalchemy driver to execute
        return self.query.compile(
            dialect=self.dialect, compile_kwargs={"literal_binds": True}
        )
