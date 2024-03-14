import logging
from functools import reduce
from typing import Optional, Union

from typeguard import typechecked

from kestrel.exceptions import UnsupportedOperatorError
from kestrel.ir.filter import (
    BoolExp,
    ExpOp,
    FComparison,
    ListComparison,
    ListOp,
    MultiComp,
    NumCompOp,
    StrComparison,
    StrCompOp,
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
    flatten_mapping,
    translate_comparison_to_native,
    reverse_mapping,
)


_logger = logging.getLogger(__name__)


Value = Union[
    int,
    float,
    str,
    list,
]


@typechecked
def _and(lhs: str, rhs: Value) -> str:
    return " AND ".join((lhs, rhs))


@typechecked
def _or(lhs: str, rhs: Value) -> str:
    return " OR ".join((lhs, rhs))


# SQL comparison operator functions
comp2func = {
    NumCompOp.EQ: "=",
    NumCompOp.NEQ: "<>",
    NumCompOp.LT: "<",
    NumCompOp.LE: "<=",
    NumCompOp.GT: ">",
    NumCompOp.GE: ">=",
    StrCompOp.EQ: "=",
    StrCompOp.NEQ: "<>",
    StrCompOp.LIKE: "LIKE",
    StrCompOp.NLIKE: "NOT LIKE",
    # UNSUPPORTED BY OpenSearch SQL: StrCompOp.MATCHES: "REGEXP",
    # UNSUPPORTED BY OpenSearch SQL: StrCompOp.NMATCHES: "NOT REGEXP",
    ListOp.IN: "IN",
    ListOp.NIN: "NOT IN",
}


def _format_value(value):
    if isinstance(value, str):
        # Need to quote string values
        value = f"'{value}'"
    elif isinstance(value, list):
        # SQL uses parens for lists
        value = tuple(value)
    return value


@typechecked
class OpenSearchTranslator:
    def __init__(
        self,
        timefmt: str,
        timestamp: str,
        select_from: str,
        data_model_map: dict,
        schema: dict,
    ):
        # Time format string for datasource
        self.timefmt = timefmt

        # Primary timestamp field in target table
        self.timestamp = timestamp

        # Query clauses
        self.table: str = select_from
        self.filt: Optional[Filter] = None
        self.entity: Optional[str] = None
        self.project: Optional[ProjectAttrs] = None
        self.limit: int = 0
        self.offset: int = 0
        self.order_by: str = ""
        self.sort_dir = SortDirection.DESC

        # Data model mapping: should be ocsf -> native
        self.from_ocsf_map = data_model_map

        # Index "schema" (field name -> type)
        self.schema = schema

    @typechecked
    def _render_comp(self, comp: FComparison) -> str:
        prefix = (
            f"{self.entity}." if (self.entity and comp.field != self.timestamp) else ""
        )
        ocsf_field = f"{prefix}{comp.field}"
        comps = translate_comparison_to_native(
            self.from_ocsf_map, ocsf_field, comp.op, comp.value
        )
        try:
            comps = [f"{f} {comp2func[o]} {_format_value(v)}" for f, o, v in comps]
            conj = " OR ".join(comps)
            result = conj if len(comps) == 1 else f"({conj})"
        except KeyError:
            raise UnsupportedOperatorError(
                comp.op.value
            )  # FIXME: need to report the mapped op, not the original
        return result

    @typechecked
    def _render_multi_comp(self, comps: MultiComp) -> str:
        op = _and if comps.op == ExpOp.AND else _or
        return reduce(op, map(self._render_comp, comps.comps))

    @typechecked
    def _render_exp(self, exp: BoolExp) -> str:
        if isinstance(exp.lhs, BoolExp):
            lhs = self._render_exp(exp.lhs)
        elif isinstance(exp.lhs, MultiComp):
            lhs = self._render_multi_comp(exp.lhs)
        else:
            lhs = self._render_comp(exp.lhs)
        if isinstance(exp.rhs, BoolExp):
            rhs = self._render_exp(exp.rhs)
        elif isinstance(exp.rhs, MultiComp):
            rhs = self._render_multi_comp(exp.rhs)
        else:
            rhs = self._render_comp(exp.rhs)
        return _and(lhs, rhs) if exp.op == ExpOp.AND else _or(lhs, rhs)

    @typechecked
    def _render_filter(self) -> Optional[str]:
        if not self.filt:
            return None
        if self.filt.timerange.start:
            # Convert the timerange to the appropriate pair of comparisons
            start_comp = StrComparison(
                self.timestamp, ">=", self.filt.timerange.start.strftime(self.timefmt)
            )
            stop_comp = StrComparison(
                self.timestamp, "<", self.filt.timerange.stop.strftime(self.timefmt)
            )
            # AND them together
            time_exp = BoolExp(start_comp, ExpOp.AND, stop_comp)
            # AND that with any existing filter expression
            exp = BoolExp(self.filt.exp, ExpOp.AND, time_exp)
        else:
            exp = self.filt.exp
        if isinstance(exp, BoolExp):
            comp = self._render_exp(exp)
        elif isinstance(exp, MultiComp):
            comp = self._render_multi_comp(exp)
        else:
            comp = self._render_comp(exp)
        return comp

    def add_Filter(self, filt: Filter) -> None:
        # Just save filter and compile it later
        # Probably need the entity projection set first
        self.filt = filt

    def add_ProjectAttrs(self, proj: ProjectAttrs) -> None:
        # Just save projection and compile it later
        self.project = proj

    def _get_fields(self) -> dict:  # TODO: rename
        # prefix = f"{self.entity}." if self.entity else ""
        entity_map = (
            self.from_ocsf_map[self.entity] if self.entity else self.from_ocsf_map
        )
        flat_map = flatten_mapping(reverse_mapping(entity_map))
        fields = {}
        for k, v in flat_map.items():
            # FIXME: ProjectAttrs in compile.py aren't mapped to OCSF, so if you use STIX it doesn't work at all
            # Check for 1:N mappings
            if isinstance(v, list):
                one_to_ones = [i for i in v if isinstance(i, str)]
                if len(one_to_ones) == 0:
                    _logger.warning("No suitable mapping for %s", k)
                    continue  # FIXME: we need to do something here
                if len(one_to_ones) > 1:
                    _logger.warning("Ambiguous mapping for %s", k)
                v = one_to_ones[0]  # TODO: how else can we choose?
            elif isinstance(v, str):
                pass  # Nothing to do?
            if self.project and not (
                v in self.project.attrs or k in self.project.attrs
            ):  # FIXME: v might be dict!!!
                # It's not in the projection, so skip it
                _logger.debug("skipping %s -> %s since it's not in projection", k, v)
                continue
            fields[k] = v

        if not fields:
            # If this is still empty, then the attr projection must be for attrs "outside" to entity projection?
            fields = {attr: attr for attr in self.project.attrs}

        _logger.debug("OCSF fields: %s", fields)
        return fields

    def _render_proj(self):
        """Get a list of native cols to project with their OCSF equivalents as SQL aliases"""
        # input is either (flat) OCSF, ECS, or STIX fields and we need to create (native, OCSF) alias mapping
        # - this may be a common capability?  Need a func to produce native:ocsf dict
        # - how to handle collisions?
        # Need access to schema to prune to fields that are actually available?
        fields = self._get_fields()
        proj = [f"`{k}` AS `{v}`" if k != v else k for k, v in fields.items()]
        _logger.debug("Set projection to %s", proj)
        return proj

    def add_ProjectEntity(self, proj: ProjectEntity) -> None:
        self.entity = proj.entity_type
        _logger.debug("Set base entity to '%s'", self.entity)

    def add_Limit(self, lim: Limit) -> None:
        self.limit = lim.num

    def add_Offset(self, offset: Offset) -> None:
        self.offset = offset.num

    def add_Sort(self, sort: Sort) -> None:
        self.order_by = sort.attribute
        self.sort_dir = sort.direction

    def add_instruction(self, i: Instruction) -> None:
        inst_name = i.instruction
        method_name = f"add_{inst_name}"
        try:
            method = getattr(self, method_name)
        except AttributeError as e:
            raise NotImplementedError(f"OpenSearchTranslator.{method_name}")
        method(i)

    def result(self) -> str:
        stages = ["SELECT"]
        cols = ", ".join(self._render_proj())
        stages.append(f"{cols}")
        stages.append(f"FROM {self.table}")
        where = self._render_filter()
        if where:
            stages.append(f"WHERE {where}")
        if self.order_by:
            stages.append(f"ORDER BY {self.order_by} {self.sort_dir.value}")
        if self.limit:
            # https://opensearch.org/docs/latest/search-plugins/sql/sql/basic/#limit
            if self.offset:
                stages.append(f"LIMIT {self.offset}, {self.limit}")
            else:
                stages.append(f"LIMIT {self.limit}")
        sql = " ".join(stages)
        _logger.debug("SQL: %s", sql)
        return sql
