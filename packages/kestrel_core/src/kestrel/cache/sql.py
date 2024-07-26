import logging
import os
from copy import copy
from tempfile import mkstemp
from typing import Any, Iterable, Mapping, MutableMapping, Optional
from uuid import UUID

import sqlalchemy
from dateutil.parser import parse as dt_parser
from kestrel.cache.base import AbstractCache
from kestrel.config.internal import VIRTUAL_CACHE_VAR_DATA
from kestrel.display import GraphletExplanation, NativeQuery
from kestrel.interface.codegen.sql import SqlTranslator
from kestrel.interface.codegen.utils import variable_attributes_to_dataframe
from kestrel.ir.graph import IRGraphEvaluable
from kestrel.ir.instructions import (
    Construct,
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
from pandas import DataFrame, read_sql
from typeguard import typechecked

_logger = logging.getLogger(__name__)


class SqlCacheTranslator(SqlTranslator):
    def __init__(self, from_obj, from_obj_schema=None, projection_base_field=None):
        super().__init__(
            sqlalchemy.dialects.sqlite.dialect(),
            from_obj,
            from_obj_schema,
            projection_base_field,
            None,
            dt_parser,
            "time",
        )  # FIXME: need mapping for timestamp?


@typechecked
class SqlCache(AbstractCache):
    def __init__(
        self,
        initial_cache: Optional[Mapping[UUID, DataFrame]] = None,
        debug: bool = False,
    ):
        super().__init__()

        if debug:
            self.db_path = "local.db"
        else:
            _, self.db_path = mkstemp(suffix=".db")

        # for an absolute file path, the three slashes are followed by the absolute path
        # for a relative path, it's also three slashes?
        self.engine = sqlalchemy.create_engine(f"sqlite:///{self.db_path}")
        self.connection = self.engine.connect()

        # besides self.cache_catalog, which stores instruction.id to table name mapping
        # we also stores instruction.id to table schema mapping for ProjectEntity use
        self.cache_catalog_schemas = {}

        if initial_cache:
            for instruction_id, data in initial_cache.items():
                self[instruction_id] = data

    def __del__(self):
        self.connection.close()
        os.remove(self.db_path)

    def __getitem__(self, instruction_id: UUID) -> DataFrame:
        return read_sql(self.cache_catalog[instruction_id], self.connection)

    def __delitem__(self, instruction_id: UUID):
        table_name = self.cache_catalog[instruction_id]
        self.connection.execute(sqlalchemy.text(f'DROP TABLE "{table_name}"'))
        del self.cache_catalog[instruction_id]

    def __setitem__(
        self,
        instruction_id: UUID,
        data: DataFrame,
    ):
        table_name = instruction_id.hex
        if table_name not in self.cache_catalog:
            self.cache_catalog[instruction_id] = table_name
            data.to_sql(
                table_name, con=self.connection, if_exists="replace", index=False
            )
            self.cache_catalog_schemas[instruction_id] = list(data)
        else:
            _logger.debug(f"instruction already cached: {instruction_id}, {data}")

    def get_virtual_copy(self) -> AbstractCache:
        v = copy(self)
        v.cache_catalog = copy(self.cache_catalog)
        v.__class__ = SqlCacheVirtual
        return v

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
            _logger.debug(f"evaluate instruction: {instruction}")
            translator = self._evaluate_instruction_in_graph(graph, instruction)
            # TODO: may catch error in case evaluation starts from incomplete SQL
            _logger.debug(f"SQL query generated: {translator.result_w_literal_binds()}")
            df = read_sql(translator.result(), self.connection)

            # handle Information command
            if isinstance(instruction, Return):
                trunk, _ = graph.get_trunk_n_branches(instruction)
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
        if not instructions_to_explain:
            instructions_to_explain = graph.get_sink_nodes()
        for instruction in instructions_to_explain:
            dep_graph = graph.duplicate_dependent_subgraph_of_node(instruction)
            graph_dict = dep_graph.to_dict()
            translator = self._evaluate_instruction_in_graph(graph, instruction)
            query = NativeQuery("SQL", str(translator.result_w_literal_binds()))
            mapping[instruction.id] = GraphletExplanation(graph_dict, query)
        return mapping

    def _evaluate_instruction_in_graph(
        self,
        graph: IRGraphEvaluable,
        instruction: Instruction,
        graph_genuine_copy: Optional[IRGraphEvaluable] = None,
        subquery_memory: Optional[Mapping[UUID, SqlCacheTranslator]] = None,
    ) -> SqlCacheTranslator:
        """Evaluate the instruction in the graph

        This method recursively traverse the graph from the instruction node to
        evaluate the instruction with all its dependencies.

        To avoid repeated traversal/evaluation of the same subgraph/subtree,
        for each Variable instruction/node, the method performs dynamic
        programming in the form of memorization subgraph results as CTEs. This
        advanced feature requires the underlying SQL engine to support common
        table expression (CTE), which may not be possible for query engines
        like SQL on OpenSearch (Kestrel OpenSearch interface uses embedded
        subquery instead of CTE).

        To avoid unexpected Python behavior
        https://docs.quantifiedcode.com/python-anti-patterns/correctness/mutable_default_value_as_argument.html
        We use `None` as default value instead of `{}`

        Parameters:
            graph: the graph to traverse, node of which will be modified during evaluation
            instruction: the instruction to evaluate/return
            graph_genuine_copy: the original graph, deep copy, no modification, for traversal use
            subquery_memory: memorize the subgraph traversed/evaluated

        Returns:
            A translator (SQL statements) to be executed
        """
        if graph_genuine_copy is None:
            graph_genuine_copy = graph.deepcopy()

        if subquery_memory is None:
            subquery_memory = {}

        if instruction.id in self:
            # cached in sqlite
            table_name = self.cache_catalog[instruction.id]
            source_schema = self.cache_catalog_schemas[instruction.id]
            translator = SqlCacheTranslator(table_name, source_schema)

        elif isinstance(instruction, SourceInstruction):
            if isinstance(instruction, Construct):
                # cache the data
                self[instruction.id] = DataFrame(instruction.data)
                # pull the data to start a SqlCacheTranslator
                table_name = self.cache_catalog[instruction.id]
                source_schema = self.cache_catalog_schemas[instruction.id]
                translator = SqlCacheTranslator(table_name, source_schema)
            else:
                raise NotImplementedError(f"Unknown instruction type: {instruction}")

        elif isinstance(instruction, TransformingInstruction):
            if instruction.id in subquery_memory:
                # this is a Variable, already evaluated
                # just create a new use/translator from this Variable
                translator = subquery_memory[instruction.id]
            else:
                trunk, r2n = graph.get_trunk_n_branches(instruction)
                translator = self._evaluate_instruction_in_graph(
                    graph, trunk, graph_genuine_copy, subquery_memory
                )

                if isinstance(instruction, SolePredecessorTransformingInstruction):
                    if isinstance(instruction, (Return, Explain)):
                        pass
                    elif isinstance(instruction, Variable):
                        subquery_memory[instruction.id] = translator
                        translator = SqlCacheTranslator(
                            translator.query.cte(name=instruction.name),
                            translator.projected_schema,
                            translator.projection_base_field,
                        )
                    else:
                        translator.add_instruction(instruction)

                elif isinstance(instruction, Filter):
                    # replace each ReferenceValue with a subquery
                    # note that this subquery will be used as a value for the .in_ operator
                    # we should not use .subquery() here but just `Select` class
                    # otherwise, will get warning:
                    #   SAWarning: Coercing Subquery object into a select() for use in IN();
                    #   please pass a select() construct explicitly
                    instruction.resolve_references(
                        lambda x: self._evaluate_instruction_in_graph(
                            graph, r2n[x], graph_genuine_copy, subquery_memory
                        ).query
                    )
                    translator.add_instruction(instruction)

                else:
                    raise NotImplementedError(
                        f"Unknown instruction type: {instruction}"
                    )

        else:
            raise NotImplementedError(f"Unknown instruction type: {instruction}")

        return translator


@typechecked
class SqlCacheVirtual(SqlCache):
    def __getitem__(self, instruction_id: UUID) -> DataFrame:
        if instruction_id in self.cache_catalog:
            try:
                df = read_sql(self.cache_catalog[instruction_id], self.connection)
            except:
                df = VIRTUAL_CACHE_VAR_DATA
        else:
            raise KeyError(instruction_id)
        return df

    def __delitem__(self, instruction_id: UUID):
        del self.cache_catalog[instruction_id]

    def __setitem__(self, instruction_id: UUID, data: DataFrame):
        self.cache_catalog[instruction_id] = instruction_id.hex + "v"
        self.cache_catalog_schemas[instruction_id] = ["*"]

    def __del__(self):
        pass
