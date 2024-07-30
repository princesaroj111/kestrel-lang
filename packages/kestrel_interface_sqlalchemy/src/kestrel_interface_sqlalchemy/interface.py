import logging
from functools import reduce
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
from uuid import UUID

import sqlalchemy
from kestrel.display import GraphletExplanation, NativeQuery
from kestrel.exceptions import InvalidDataSource
from kestrel.interface import DatasourceInterface
from kestrel.interface.codegen.sql import ingest_dataframe_to_temp_table
from kestrel.interface.codegen.utils import variable_attributes_to_dataframe
from kestrel.ir.graph import IRGraphEvaluable
from kestrel.ir.instructions import (
    DataSource,
    Explain,
    Filter,
    Information,
    Instruction,
    Return,
    SolePredecessorTransformingInstruction,
    SourceInstruction,
    TransformingInstruction,
    Variable,
)
from kestrel.mapping.data_model import translate_dataframe
from pandas import DataFrame, read_sql
from typeguard import typechecked

from .config import load_config
from .translator import NativeTable, SQLAlchemyTranslator, SubQuery

_logger = logging.getLogger(__name__)


@typechecked
class SQLAlchemyInterface(DatasourceInterface):
    def __init__(
        self,
        serialized_cache_catalog: Optional[str] = None,
        session_id: Optional[UUID] = None,
    ):
        _logger.debug("SQLAlchemyInterface: loading config")
        super().__init__(serialized_cache_catalog, session_id)
        self.engines: dict = {}  # Map of conn name -> engine
        self.config = load_config()
        for info in self.config.datasources.values():
            name = info.connection
            conn_info = self.config.connections[name]
            if name not in self.engines:
                self.engines[name] = sqlalchemy.create_engine(conn_info.url)
            _logger.debug("SQLAlchemyInterface: configured %s", name)

    @staticmethod
    def schemes() -> Iterable[str]:
        return ["sqlalchemy"]

    def get_datasources(self) -> List[str]:
        return list(self.config.datasources)

    def get_storage_of_datasource(self, datasource: str) -> str:
        """Get the storage name of a given datasource"""
        if datasource not in self.config.datasources:
            raise InvalidDataSource(datasource)
        return self.config.datasources[datasource].connection

    def store(
        self,
        instruction_id: UUID,
        data: DataFrame,
    ):
        raise NotImplementedError("SQLAlchemyInterface.store")  # TEMP

    def __del__(self):
        pass

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
            # connection is renewed for each instruction to evaluate
            conn = self.engines[graph.store].connect()
            # we also need a clean copy of the graph for reference resolution
            # otherwise, we will refer to the subquery in previous for iteration
            # which will give empty results
            _graph = graph.deepcopy()

            translator = self._evaluate_instruction_in_graph(
                conn, _graph, cache, instruction
            )
            # TODO: may catch error in case evaluation starts from incomplete SQL
            sql = translator.result()
            _logger.debug("SQL query generated: %s", sql)

            # Get the "from" table for this query
            df = read_sql(sql, conn)
            conn.close()

            # value translation
            if translator.data_mapping:

                if translator.projection_base_field == "event":
                    dmm = translator.data_mapping
                else:
                    try:
                        dmm = reduce(
                            dict.__getitem__,
                            translator.projection_base_field.split("."),
                            translator.data_mapping,
                        )
                    except KeyError:
                        # pass through
                        _logger.debug("No result/value translation")
                        dmm = None

                df = translate_dataframe(df, dmm) if dmm else df

            # handle Information command
            if isinstance(instruction, Return):
                trunk = (
                    instruction.predecessor
                    if hasattr(instruction, "predecessor")
                    else _graph.get_trunk_n_branches(instruction)[0]
                )
                if isinstance(trunk, Information):
                    df = variable_attributes_to_dataframe(df)

            mapping[instruction.id] = df
        return mapping

    def explain_graph(
        self,
        graph: IRGraphEvaluable,
        cache: MutableMapping[UUID, Any],
        instructions_to_explain: Optional[Iterable[Instruction]] = None,
    ) -> Mapping[UUID, GraphletExplanation]:
        mapping = {}
        graph_genuine_copy = graph.deepcopy()
        if not instructions_to_explain:
            instructions_to_explain = graph.get_sink_nodes()
        for instruction in instructions_to_explain:
            # duplicate graph here before ref resolution
            dep_graph = graph_genuine_copy.duplicate_dependent_subgraph_of_node(
                instruction
            )
            # render the graph in SQL
            conn = self.engines[graph.store].connect()
            translator = self._evaluate_instruction_in_graph(
                conn, graph, cache, instruction
            )
            query = NativeQuery("SQL", str(translator.result_w_literal_binds()))
            conn.close()
            # return the graph and SQL
            mapping[instruction.id] = GraphletExplanation(dep_graph.to_dict(), query)
        return mapping

    def _evaluate_instruction_in_graph(
        self,
        conn: sqlalchemy.engine.Connection,
        graph: IRGraphEvaluable,
        cache: MutableMapping[UUID, Any],
        instruction: Instruction,
        graph_genuine_copy: Optional[IRGraphEvaluable] = None,
        subquery_memory: Optional[Mapping[UUID, SQLAlchemyTranslator]] = None,
    ) -> SQLAlchemyTranslator:
        # if method name needs update/change, also update for the `inspect`
        # if any parameter name needs update/change, also update for the `inspect`

        _logger.debug("instruction: %s", str(instruction))

        if graph_genuine_copy is None:
            graph_genuine_copy = graph.deepcopy()

        # same use as `subquery_memory` in `kestrel.cache.sql`
        if subquery_memory is None:
            subquery_memory = {}

        if instruction.id in cache:
            if instruction.id in subquery_memory:
                translator = subquery_memory[instruction.id]
            else:
                if self.config.connections[graph.store].table_creation_permission:
                    table_name = instruction.id.hex

                    # write to temp table
                    ingest_dataframe_to_temp_table(
                        conn,
                        cache[instruction.id],
                        table_name,
                    )

                    # SELECT * from the new table
                    translator = SQLAlchemyTranslator(
                        NativeTable(
                            self.engines[graph.store].dialect,
                            table_name,
                            list(cache[instruction.id]),
                            None,
                            None,
                            None,
                        )
                    )
                    subquery_memory[instruction.id] = translator

                else:
                    raise NotImplementedError("Read-only data lake not handled")
                    # list(cache[instruction.id].itertuples(index=False, name=None))

        if isinstance(instruction, SourceInstruction):
            if isinstance(instruction, DataSource):
                ds_config = self.config.datasources[instruction.datasource]
                columns = list(
                    conn.execute(
                        sqlalchemy.text(f"SELECT * FROM {ds_config.table} LIMIT 1")
                    ).keys()
                )
                translator = SQLAlchemyTranslator(
                    NativeTable(
                        self.engines[ds_config.connection].dialect,
                        ds_config.table,
                        columns,
                        ds_config.data_model_map,
                        lambda dt: dt.strftime(ds_config.timestamp_format),
                        ds_config.timestamp,
                    )
                )
            else:
                raise NotImplementedError(f"Unhandled instruction type: {instruction}")

        elif isinstance(instruction, TransformingInstruction):
            if instruction.id in subquery_memory:
                translator = subquery_memory[instruction.id]
            else:
                # record the predecessor so we do not resolve reference for Filter again
                # which is not possible (ReferenceValue already gone---replaced with subquery)
                if hasattr(instruction, "predecessor"):
                    trunk, r2n = instruction.predecessor, {}
                else:
                    trunk, r2n = graph.get_trunk_n_branches(instruction)
                    instruction.predecessor = trunk

                translator = self._evaluate_instruction_in_graph(
                    conn, graph, cache, trunk, graph_genuine_copy, subquery_memory
                )

                if isinstance(instruction, SolePredecessorTransformingInstruction):
                    if isinstance(instruction, (Return, Explain)):
                        pass
                    elif isinstance(instruction, Variable):
                        subquery_memory[instruction.id] = translator
                        translator = SQLAlchemyTranslator(
                            SubQuery(translator, instruction.name)
                        )
                    else:
                        translator.add_instruction(instruction)

                elif isinstance(instruction, Filter):
                    if r2n:
                        instruction.resolve_references(
                            lambda x: self._evaluate_instruction_in_graph(
                                conn,
                                graph,
                                cache,
                                r2n[x],
                                graph_genuine_copy,
                                subquery_memory,
                            ).query
                        )
                    translator.add_instruction(instruction)

                else:
                    raise NotImplementedError(
                        f"Unknown instruction type: {instruction}"
                    )

        return translator
