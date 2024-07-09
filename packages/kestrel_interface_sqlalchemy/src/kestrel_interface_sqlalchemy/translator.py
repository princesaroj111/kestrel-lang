import logging
from functools import reduce
from typing import Callable, Optional, Union
from typeguard import typechecked

import sqlalchemy
from sqlalchemy import column, or_, tuple_
from sqlalchemy.sql.expression import CTE

from kestrel.interface.codegen.sql import SqlTranslator, comp2func
from kestrel.ir.filter import (
    RefComparison,
    FBasicComparison,
    StrCompOp,
)
from kestrel.ir.instructions import (
    Filter,
    ProjectAttrs,
    ProjectEntity,
)
from kestrel.mapping.data_model import (
    translate_comparison_to_native,
    translate_projection_to_native,
)


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
