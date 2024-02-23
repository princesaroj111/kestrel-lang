import logging
from typing import Iterable, Mapping, Optional, Union
from uuid import UUID

import sqlalchemy
from dateutil.parser import parse as dt_parser
from pandas import DataFrame, read_sql
from typeguard import typechecked

from kestrel.cache.base import AbstractCache
from kestrel.interface.datasource.codegen.sql import SqlTranslator
from kestrel.ir.graph import IRGraphEvaluable
from kestrel.ir.instructions import (
    Construct,
    Instruction,
    Return,
    Variable,
    Filter,
    SourceInstruction,
    TransformingInstruction,
    SolePredecessorTransformingInstruction,
)

_logger = logging.getLogger(__name__)


@typechecked
class SqliteTranslator(SqlTranslator):
    def __init__(self, from_obj: Union[SqlTranslator, str]):
        if isinstance(from_obj, SqlTranslator):
            fc = from_obj.query.subquery()
        else:  # str to represent table name
            fc = sqlalchemy.table(from_obj)
        super().__init__(
            sqlalchemy.dialects.sqlite.dialect(), dt_parser, "time", fc
        )  # FIXME: need mapping for timestamp?


@typechecked
class SqliteCache(AbstractCache):
    def __init__(
        self,
        initial_cache: Optional[Mapping[UUID, DataFrame]] = None,
        session_id: Optional[UUID] = None,
    ):
        super().__init__()

        basename = self.session_id or "cache"
        path = f"{basename}.db"

        # for an absolute file path, the three slashes are followed by the absolute path
        # for a relative path, it's also three slashes?
        self.engine = sqlalchemy.create_engine(f"sqlite:///{path}")
        self.connection = self.engine.connect()

        if initial_cache:
            for instruction_id, data in initial_cache.items():
                self[instruction_id] = data

    def __del__(self):
        self.connection.close()

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
        self.cache_catalog[instruction_id] = table_name
        data.to_sql(table_name, con=self.connection, if_exists="replace", index=False)

    def evaluate_graph(
        self,
        graph: IRGraphEvaluable,
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
            mapping[instruction.id] = read_sql(translator.result(), self.connection)
        return mapping

    def _evaluate_instruction_in_graph(
        self,
        graph: IRGraphEvaluable,
        instruction: Instruction,
    ) -> SqliteTranslator:
        if instruction.id in self:
            # cached in sqlite
            table_name = self.cache_catalog[instruction.id]
            translator = SqliteTranslator(table_name)

        elif isinstance(instruction, SourceInstruction):
            if isinstance(instruction, Construct):
                # cache the data
                self[instruction.id] = DataFrame(instruction.data)
                # pull the data to start a SqliteTranslator
                table_name = self.cache_catalog[instruction.id]
                translator = SqliteTranslator(table_name)
            else:
                raise NotImplementedError(f"Unknown instruction type: {instruction}")

        elif isinstance(instruction, TransformingInstruction):
            trunk, r2n = graph.get_trunk_n_branches(instruction)
            translator = self._evaluate_instruction_in_graph(graph, trunk)

            if isinstance(instruction, SolePredecessorTransformingInstruction):
                if isinstance(instruction, Return):
                    pass
                elif isinstance(instruction, Variable):
                    # start a new translator and use previous one as subquery
                    # this allows using the variable as a dependent node
                    # if the variable is a sink, `SELECT * FROM (subquery)` also works
                    translator = SqliteTranslator(translator)
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
                    lambda x: self._evaluate_instruction_in_graph(graph, r2n[x]).query
                )
                translator.add_instruction(instruction)

            else:
                raise NotImplementedError(f"Unknown instruction type: {instruction}")

        else:
            raise NotImplementedError(f"Unknown instruction type: {instruction}")

        return translator
