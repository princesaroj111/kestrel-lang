import logging
from functools import reduce
from typing import Any, Iterable, Mapping, MutableMapping, Optional
from uuid import UUID
from typeguard import typechecked
import sqlalchemy
from pandas import DataFrame, read_sql

from kestrel.display import GraphletExplanation
from kestrel.interface import AbstractInterface
from kestrel.ir.graph import IRGraphEvaluable
from kestrel.ir.instructions import (
    DataSource,
    Filter,
    Instruction,
    Return,
    Explain,
    SolePredecessorTransformingInstruction,
    SourceInstruction,
    TransformingInstruction,
    Variable,
)
from kestrel.mapping.data_model import translate_dataframe
from kestrel.exceptions import SourceNotFound

from .translator import SQLAlchemyTranslator
from .utils import iter_argument_from_function_in_callstack
from .config import load_config


_logger = logging.getLogger(__name__)


@typechecked
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
            # 1. get the datasource assocaited with the cached node
            ds = None
            for node in iter_argument_from_function_in_callstack(
                "_evaluate_instruction_in_graph", "instruction"
            ):
                try:
                    ds = graph.find_datasource_of_node(node)
                except SourceNotFound:
                    continue
                else:
                    break
            if not ds:
                _logger.error(
                    "backed tracked entire stack but still do not find source"
                )
                raise SourceNotFound(instruction)

            # 2. check the datasource config to see if the datalake supports write
            ds_config = self.config.datasources[ds.datasource]
            conn_config = self.config.connections[ds_config.connection]

            # 3. perform table creation or in-memory cache
            if conn_config.table_creation_permission:
                table_name = "kestrel_temp_" + instruction.id.hex

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
