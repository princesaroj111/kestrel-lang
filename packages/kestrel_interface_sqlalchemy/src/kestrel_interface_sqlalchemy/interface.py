import logging
import inspect
from functools import reduce
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional, Union
from uuid import UUID

import sqlalchemy
from kestrel_interface_sqlalchemy.config import load_config
from pandas import DataFrame, read_sql
from sqlalchemy import column, or_, tuple_
from sqlalchemy.sql.expression import CTE
from typeguard import typechecked

from kestrel.display import GraphletExplanation
from kestrel.interface import AbstractInterface
from kestrel.interface.codegen.sql import SqlTranslator, comp2func
from kestrel.ir.filter import (
    BoolExp,
    ExpOp,
    RefComparison,
    FBasicComparison,
    MultiComp,
    StrComparison,
    StrCompOp,
)
from kestrel.ir.graph import IRGraphEvaluable
from kestrel.ir.instructions import (
    DataSource,
    Filter,
    Instruction,
    ProjectAttrs,
    ProjectEntity,
    Return,
    SolePredecessorTransformingInstruction,
    SourceInstruction,
    TransformingInstruction,
    Variable,
)
from kestrel.mapping.data_model import (
    translate_comparison_to_native,
    translate_dataframe,
    translate_projection_to_native,
)
from kestrel.exceptions import SourceNotFound

_logger = logging.getLogger(__name__)


@typechecked
class SQLAlchemyTranslator(SqlTranslator):
    def __init__(
        self,
        dialect: sqlalchemy.engine.default.DefaultDialect,
        from_obj: Union[CTE, str],
        dmm: Optional[dict] = None,  # CTE does not have dmm
        timefmt: Optional[Callable] = None,  # CTE does not have timefmt
        timestamp: Optional[str] = None,  # CTE does not have timestamp
    ):
        if isinstance(from_obj, CTE):
            fc = from_obj
        else:  # str to represent table name
            fc = sqlalchemy.table(from_obj)
        super().__init__(dialect, fc, timefmt, timestamp)
        self.dmm = dmm
        self.projection_attributes = None
        self.projection_base_field = None
        self.filt: Filter = None

    @typechecked
    def _render_comp(self, comp: FBasicComparison):
        if isinstance(comp, RefComparison):
            # no translation for CTE/subquery (RefComparison)
            # the results should already be in OCSF in a variable (CTE)
            if len(comp.fields) == 1:
                col = column(comp.fields[0])
            else:
                col = tuple_(*[column(field) for field in comp.fields])
            rendered_comp = comp2func[comp.op](col, comp.value)
        else:
            # do translation from a raw database table
            prefix = (
                f"{self.projection_base_field}."
                if (self.projection_base_field and comp.field != self.timestamp)
                else ""
            )
            ocsf_field = f"{prefix}{comp.field}"
            comps = translate_comparison_to_native(
                self.dmm, ocsf_field, comp.op, comp.value
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
        return rendered_comp

    def add_Filter(self, filt: Filter) -> None:
        # Just save filter and compile it later
        # Probably need the entity projection set first
        self.filt = filt

    def add_ProjectAttrs(self, proj: ProjectAttrs) -> None:
        self.projection_attributes = proj.attrs

    def add_ProjectEntity(self, proj: ProjectEntity) -> None:
        self.projection_base_field = proj.ocsf_field

    def result(self) -> sqlalchemy.Compiled:

        # 1. process the filter
        if self.filt:
            selection = self.filter_to_selection(self.filt)
            self.query = self.query.where(selection)

        # 2. process projections
        if self.dmm:
            # translation required
            # basically this is not from a subquery/CTE (already normalized)
            # it is possible: self.projection_base_field is None (will use root)
            # it is possible: self.projection_attributes is None (will translate all)
            pairs = translate_projection_to_native(
                self.dmm, self.projection_base_field, self.projection_attributes
            )
            cols = [sqlalchemy.column(i).label(j) for i, j in pairs]
        elif self.projection_attributes:
            cols = [sqlalchemy.column(i) for i in self.projection_attributes]
        else:
            # if projection_attributes not specified, `SELECT *` (default option)
            # this can happen if the table is loaded cached ProjectAttrs
            # or just a Kestrel expression on a varaible (Filter only)
            cols = None

        if cols is not None:
            self.query = self.query.with_only_columns(*cols)  # TODO: mapping?

        # 3. return compiled result
        return self.query.compile(dialect=self.dialect)


class SQLAlchemyInterface(AbstractInterface):
    def __init__(
        self,
        serialized_cache_catalog: Optional[str] = None,
        session_id: Optional[UUID] = None,
    ):
        _logger.debug("SQLAlchemyInterface: loading config")
        super().__init__(serialized_cache_catalog, session_id)
        self.config = load_config()
        self.schemas: dict = {}  # Schema per table (index)
        self.engines: dict = {}  # Map of conn name -> engine
        self.conns: dict = {}  # Map of conn name -> connection
        for info in self.config.datasources.values():
            name = info.connection
            conn_info = self.config.connections[name]
            if name not in self.engines:
                self.engines[name] = sqlalchemy.create_engine(conn_info.url)
            if name not in self.conns:
                engine = self.engines[name]
                self.conns[name] = engine.connect()
            _logger.debug("SQLAlchemyInterface: configured %s", name)

    @staticmethod
    def schemes() -> Iterable[str]:
        return ["sqlalchemy"]

    def store(
        self,
        instruction_id: UUID,
        data: DataFrame,
    ):
        raise NotImplementedError("SQLAlchemyInterface.store")  # TEMP

    def evaluate_graph(
        self,
        graph: IRGraphEvaluable,
        cache: MutableMapping[UUID, Any],
        instructions_to_evaluate: Optional[Iterable[Instruction]] = None,
    ) -> Mapping[UUID, DataFrame]:
        mapping = {}
        if not instructions_to_evaluate:
            instructions_to_evaluate = graph.get_sink_nodes()
        for instruction in instructions_to_evaluate:
            translator = self._evaluate_instruction_in_graph(graph, cache, instruction)
            # TODO: may catch error in case evaluation starts from incomplete SQL
            sql = translator.result()
            _logger.debug("SQL query generated: %s", sql)
            # Get the "from" table for this query
            tables = translator.query.selectable.get_final_froms()
            table = tables[0].name  # TODO: what if there's more than 1?
            # Get the data source's SQLAlchemy connection object
            conn = self.conns[self.config.datasources[table].connection]
            df = read_sql(sql, conn)
            entity_dmm = reduce(
                dict.__getitem__, translator.projection_base_field.split("."), dmm
            )
            mapping[instruction.id] = translate_dataframe(df, entity_dmm)
        return mapping

    def explain_graph(
        self,
        graph: IRGraphEvaluable,
        cache: MutableMapping[UUID, Any],
        instructions_to_explain: Optional[Iterable[Instruction]] = None,
    ) -> Mapping[UUID, GraphletExplanation]:
        mapping = {}
        if not instructions_to_explain:
            instructions_to_explain = graph.get_sink_nodes()
        for instruction in instructions_to_explain:
            translator = self._evaluate_instruction_in_graph(graph, cache, instruction)
            dep_graph = graph.duplicate_dependent_subgraph_of_node(instruction)
            graph_dict = dep_graph.to_dict()
            query_stmt = translator.result()
            mapping[instruction.id] = GraphletExplanation(graph_dict, query_stmt)
        return mapping

    def _evaluate_instruction_in_graph(
        self,
        graph: IRGraphEvaluable,
        cache: MutableMapping[UUID, Any],
        instruction: Instruction,
        subquery_memory: Optional[Mapping[UUID, SQLAlchemyTranslator]] = None,
    ) -> SQLAlchemyTranslator:
        # if method name needs update/change, also update for the `inspect`
        # if any parameter name needs update/change, also update for the `inspect`

        _logger.debug("instruction: %s", str(instruction))

        # same use as `subquery_memory` in `kestrel.cache.sql`
        if subquery_memory is None:
            subquery_memory = {}

        if instruction.id in cache:
            # first get the datasource assocaited with the cached node
            ds = None
            x_layer_backed = 1
            while not ds:
                caller = inspect.stack()[x_layer_backed]
                if caller[3] != "_evaluate_instruction_in_graph":
                    # back tracked too far
                    break
                successor = caller[0].f_locals["instruction"]
                try:
                    ds = graph.get_datasource_of_node(successor)
                except SourceNotFound:
                    x_layer_backed += 1
            if not ds:
                _logger.error(
                    "backed tracked entire stack but still do not find source"
                )
                raise SourceNotFound(instruction)

            # then check the datasource config to see if the datalake supports write
            ds_config = self.config.datasources[ds.datasource]
            conn_config = self.config.connections[ds_config.connection]

            if conn_config.table_creation_permission:
                table_name = instruction.id

                # create a new table for the cached DataFrame
                cache[instruction.id].to_sql(
                    table_name,
                    con=self.conns[ds_config.connection],
                    if_exists="replace",
                    index=False,
                )

                # SELECT * from the new table
                translator = SQLAlchemyTranslator(
                    self.engines[ds_config.connection].dialect,
                    table_name,
                    None,
                    None,
                    None,
                )

            else:
                raise NotImplementedError("Read-only data lake not handled")
                # list(cache[instruction.id].itertuples(index=False, name=None))

        if isinstance(instruction, SourceInstruction):
            if isinstance(instruction, DataSource):
                ds_config = self.config.datasources[instruction.datasource]
                translator = SQLAlchemyTranslator(
                    self.engines[ds_config.connection].dialect,
                    ds_config.table,
                    ds_config.data_model_map,
                    lambda dt: dt.strftime(ds_config.timestamp_format),
                    ds_config.timestamp,
                )
            else:
                raise NotImplementedError(f"Unhandled instruction type: {instruction}")

        elif isinstance(instruction, TransformingInstruction):
            if instruction.id in subquery_memory:
                translator = subquery_memory[instruction.id]
            else:
                trunk, r2n = graph.get_trunk_n_branches(instruction)
                translator = self._evaluate_instruction_in_graph(
                    graph, cache, trunk, subquery_memory
                )

                if isinstance(instruction, SolePredecessorTransformingInstruction):
                    if isinstance(instruction, (Return, Explain)):
                        pass
                    elif isinstance(instruction, Variable):
                        subquery_memory[instruction.id] = translator
                        translator = SQLAlchemyTranslator(
                            translator.dialect,
                            translator.query.cte(name=instruction.name),
                        )
                    else:
                        translator.add_instruction(instruction)

                elif isinstance(instruction, Filter):
                    instruction.resolve_references(
                        lambda x: self._evaluate_instruction_in_graph(
                            graph, cache, r2n[x], cte_memory
                        ).query
                    )
                    translator.add_instruction(instruction)

                else:
                    raise NotImplementedError(
                        f"Unknown instruction type: {instruction}"
                    )

        return translator
