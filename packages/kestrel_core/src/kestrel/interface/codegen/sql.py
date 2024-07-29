import logging
from collections import defaultdict
from functools import reduce
from typing import Callable, List, Optional, Union

import sqlalchemy
from kestrel.exceptions import (
    EntityNotFound,
    InvalidAttributes,
    InvalidMappingWithMultipleIdentifierFields,
    InvalidProjectEntityFromEntity,
    SourceSchemaNotFound,
)
from kestrel.ir.filter import (
    AbsoluteTrue,
    BoolExp,
    ExpOp,
    FBasicComparison,
    ListOp,
    MultiComp,
    NumCompOp,
    RefComparison,
    StrComparison,
    StrCompOp,
)
from kestrel.ir.instructions import (
    Filter,
    Information,
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
from kestrel.mapping.utils import get_type_from_projection
from pandas import DataFrame
from pandas.io.sql import SQLTable, pandasSQL_builder
from sqlalchemy import and_, asc, column, desc, or_, select, tuple_
from sqlalchemy.engine import Compiled, Connection, default
from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList
from sqlalchemy.sql.expression import CTE, ColumnElement, ColumnOperators
from sqlalchemy.sql.functions import coalesce
from sqlalchemy.sql.selectable import Select
from typeguard import typechecked

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
class _TemporaryTable(SQLTable):
    def _execute_create(self):
        self.table = self.table.to_metadata(self.pd_sql.meta)
        self.table._prefixes.append("TEMPORARY")
        with self.pd_sql.run_transaction():
            self.table.create(bind=self.pd_sql.con)

    def create(self) -> None:
        # SQLite actually use the same temp DB everytime
        # even after connection closed and opened again
        # so we need to drop the previous temp table
        #
        # override the superclass code here
        # since we need to drop a table with a different schema
        # which is not supported by the superclass method
        if self.exists():
            with self.pd_sql.run_transaction():
                self.pd_sql.get_table(self.name).drop(bind=self.pd_sql.con)
                self.pd_sql.meta.clear()
        self._execute_create()


@typechecked
def ingest_dataframe_to_temp_table(conn: Connection, df: DataFrame, table_name: str):
    with pandasSQL_builder(conn) as pandas_engine:
        # no need to put if_exists="replace"
        # since our customized .create() only has this logic
        table = _TemporaryTable(table_name, pandas_engine, frame=df, index=False)
        table.create()
        df.to_sql(table_name, con=conn, if_exists="append", index=False)


def get_proj_cols(pairs):
    # Check for alias "collisions"
    alias_map = defaultdict(list)
    for i, j in pairs:
        alias_map[j].append(i)
    result = []
    coalesced = set()
    for i, j in pairs:
        if j in coalesced:
            continue  # Already handled
        native_fields = alias_map[j]
        if len(native_fields) > 1:
            # Multiple native fields are mapped to same alias - coalesce
            result.append(
                coalesce(*(sqlalchemy.column(field) for field in native_fields)).label(
                    j
                )
            )
            coalesced.add(j)
        else:
            result.append(sqlalchemy.column(i).label(j))
    return result


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

        # schema after projection
        # pass through the schema if no add_ProjectEntity()
        self.projected_schema = from_obj_schema

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

        if isinstance(from_obj, CTE):
            from_clause = from_obj
            self.is_subquery = True
        else:
            from_clause = sqlalchemy.table(from_obj)
            self.is_subquery = False

        # SQLAlchemy statement object
        # Auto-dedup by default
        self.query: Select = select("*").select_from(from_clause).distinct()

    @typechecked
    def _map_identifier_field(self, field) -> ColumnElement:
        if self.data_mapping:
            comps = translate_comparison_to_native(self.data_mapping, field, "", None)
            if len(comps) > 1:
                raise InvalidMappingWithMultipleIdentifierFields(comps)
            else:
                col = column(comps[0][0])
        else:
            col = column(field)
        return col

    @typechecked
    def _render_comp(self, comp: FBasicComparison) -> BinaryExpression:
        if isinstance(comp, RefComparison):
            # most FBasicComparison has .field; RefComparison has .fields
            # col: ColumnElement
            if len(comp.fields) == 1:
                col = self._map_identifier_field(comp.fields[0])
            else:
                col = tuple_(
                    *[self._map_identifier_field(field) for field in comp.fields]
                )
            rendered_comp = comp2func[comp.op](col, comp.value)
        elif self.data_mapping:
            comps = translate_comparison_to_native(
                self.data_mapping, comp.field, comp.op, comp.value
            )
            if self.is_subquery:
                # do not translate field
                # only translate value
                comps = [(comp.field, op, value) for (_, op, value) in comps]

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
    def _render_multi_comp(
        self, comps: MultiComp
    ) -> Union[BooleanClauseList, BinaryExpression]:
        op = and_ if comps.op == ExpOp.AND else or_
        binary_expressions = list(map(self._render_comp, comps.comps))

        # dedup using the SQLAlchemy's .compare() method; __eq__ does not work
        final_result = []
        for be in binary_expressions:
            for ue in final_result:
                if ue.compare(be):
                    break
            else:
                final_result.append(be)

        return reduce(op, final_result)

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
        if not self.source_schema:
            raise SourceSchemaNotFound(self.result_w_literal_binds())
        else:
            if self.source_schema != ["*"]:
                invalid_attrs = set(proj.attrs) - set(self.source_schema)
                if invalid_attrs:
                    raise InvalidAttributes(list(invalid_attrs))
            cols = [column(col) for col in proj.attrs]
            self.query = self.query.with_only_columns(*cols)

    def add_ProjectEntity(self, proj: ProjectEntity) -> None:
        if self.projection_base_field and self.projection_base_field != "event":
            raise InvalidProjectEntityFromEntity(proj, self.projection_base_field)
        else:
            self.projection_base_field = proj.ocsf_field

        if proj.ocsf_field == "event":  # project to event
            if self.data_mapping and not self.is_subquery:
                if not self.source_schema:
                    raise SourceSchemaNotFound(self.result_w_literal_binds())
                else:
                    pairs = translate_projection_to_native(
                        self.data_mapping, None, None, self.source_schema
                    )
            else:
                pairs = None
                _logger.debug("no data mapping, no translation for projection (event)")

        else:  # project to entity
            if not self.source_schema:
                raise SourceSchemaNotFound(self.result_w_literal_binds())

            if self.data_mapping and not self.is_subquery:
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
            if not pairs and self.source_schema != ["*"]:
                # self.source_schema == ["*"] is used in virtual cache (EXPLAIN)
                entity_type = get_type_from_projection(proj.ocsf_field)
                raise EntityNotFound(
                    f"No '{entity_type}' found at '{proj.ocsf_field}.*' against the data source."
                )

        if pairs:
            self.projected_schema = [ocsf_field for _, ocsf_field in pairs]
            _logger.debug(f"column projection pairs: {pairs}")
            cols = get_proj_cols(pairs)
            self.query = self.query.with_only_columns(*cols)

    def add_Limit(self, lim: Limit) -> None:
        self.query = self.query.limit(lim.num)

    def add_Information(self, ins: Information) -> None:
        self.query = self.query.limit(1)

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
