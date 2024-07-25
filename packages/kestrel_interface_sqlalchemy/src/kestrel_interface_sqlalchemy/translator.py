from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, List, Optional, Union

from kestrel.interface.codegen.sql import SqlTranslator
from sqlalchemy.engine.default import DefaultDialect
from typeguard import typechecked

_logger = logging.getLogger(__name__)


@dataclass
class NativeTable:
    dialect: DefaultDialect
    table_name: str
    table_schema: Optional[List[str]]  # column names
    data_model_map: Optional[dict]
    timefmt: Optional[Callable]
    timestamp: Optional[str]


@dataclass
class SubQuery:
    translator: SqlTranslator
    name: str


@typechecked
class SQLAlchemyTranslator(SqlTranslator):
    def __init__(
        self,
        obj: Union[NativeTable, SubQuery],
    ):
        if isinstance(obj, SubQuery):
            dialect = obj.translator.dialect
            from_obj = obj.translator.query.cte(name=obj.name)
            from_obj_schema = obj.translator.projected_schema
            from_obj_projection_base_field = obj.translator.projection_base_field
            ocsf_to_native_mapping = obj.translator.data_mapping
            timefmt = None
            timestamp = None

        elif isinstance(obj, NativeTable):
            dialect = obj.dialect
            from_obj = obj.table_name
            from_obj_schema = obj.table_schema
            from_obj_projection_base_field = None
            ocsf_to_native_mapping = obj.data_model_map
            timefmt = obj.timefmt
            timestamp = obj.timestamp

        else:
            raise NotImplementedError("Type not defined in argument")

        super().__init__(
            dialect,
            from_obj,
            from_obj_schema,
            from_obj_projection_base_field,
            ocsf_to_native_mapping,
            timefmt,
            timestamp,
        )
