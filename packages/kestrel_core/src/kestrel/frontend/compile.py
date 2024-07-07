# Lark Transformer

import logging
from datetime import datetime, timedelta, timezone
from functools import reduce

from dateutil.parser import parse as to_datetime
from lark import Token, Transformer
from typeguard import typechecked
from typing import Union, List

from kestrel.exceptions import IRGraphMissingNode, InvalidComparison
from kestrel.ir.filter import (
    BoolExp,
    ExpOp,
    FComparison,
    FExpression,
    FloatComparison,
    IntComparison,
    ListComparison,
    ListOp,
    MultiComp,
    NumCompOp,
    RefComparison,
    ReferenceValue,
    StrComparison,
    StrCompOp,
    TimeRange,
)
from kestrel.ir.graph import IRGraph, compose
from kestrel.ir.instructions import (
    Analytic,
    AnalyticsInterface,
    Construct,
    DataSource,
    Explain,
    Filter,
    Instruction,
    Limit,
    Offset,
    ProjectAttrs,
    ProjectEntity,
    Reference,
    Return,
    Sort,
    Variable,
)
from kestrel.mapping.data_model import (
    translate_comparison_to_ocsf,
    translate_entity_projection_to_ocsf,
    translate_attributes_projection_to_ocsf,
)
from kestrel.utils import unescape_quoted_string

_logger = logging.getLogger(__name__)


DEFAULT_VARIABLE = "_"
DEFAULT_SORT_ORDER = "DESC"


@typechecked
def _unescape_quoted_string(s: str):
    if s.startswith("r"):
        return s[2:-1]
    else:
        return s[1:-1].encode("utf-8").decode("unicode_escape")


@typechecked
def _trim_ocsf_event_field(field: str) -> str:
    """Remove event name (as prefix) if the field starts with an event"""
    items = field.split(".")
    if items[0].endswith("_event") or items[0].endswith("_activity"):
        return ".".join(items[1:])
    else:
        return field


@typechecked
def _create_comp(
    field: str, op_value: str, value: Union[str, int, float, List, ReferenceValue]
) -> FComparison:
    # TODO: implement MultiComp

    if op_value in (ListOp.IN, ListOp.NIN):
        op = ListOp
        compf = RefComparison if isinstance(value, ReferenceValue) else ListComparison
    elif isinstance(value, int):
        op = NumCompOp
        compf = IntComparison
    elif isinstance(value, float):
        op = NumCompOp
        compf = FloatComparison
    elif isinstance(value, ReferenceValue):
        op = ListOp
        op_value = ListOp.IN if op_value in (ListOp.IN, StrCompOp.EQ) else ListOp.NIN
        compf = RefComparison
    else:
        op = StrCompOp
        compf = StrComparison

    if compf is RefComparison:
        comp = compf([_trim_ocsf_event_field(field)], op(op_value), value)
    else:
        comp = compf(_trim_ocsf_event_field(field), op(op_value), value)

    return comp


@typechecked
def _map_filter_exp(
    native_projection_field: str,
    ocsf_projection_field: str,
    filter_exp: FExpression,
    field_map: dict,
) -> FExpression:
    if isinstance(
        filter_exp,
        (IntComparison, FloatComparison, StrComparison, ListComparison, RefComparison),
    ):
        # get the field/key
        if hasattr(filter_exp, "field"):
            field = filter_exp.field
        elif hasattr(filter_exp, "fields"):
            if len(filter_exp.fields) > 1:
                raise NotImplementedError(
                    "Kestrel syntax does not support fields tuple yet"
                )
            field = filter_exp.fields[0]
        else:
            raise InvalidComparison(filter_exp)
        # init map_result from direct mapping from field
        map_result = set(
            translate_comparison_to_ocsf(
                field_map, field, filter_exp.op, filter_exp.value
            )
        )
        # there is a case that `field` omits the return entity (prefix)
        # this is only alloed when it refers to the return entity
        # add mapping for those cases
        for full_field in (
            f"{native_projection_field}:{field}",
            f"{native_projection_field}.{field}",
        ):
            map_result |= set(
                filter(
                    lambda x: x[0].startswith(ocsf_projection_field + "."),
                    translate_comparison_to_ocsf(
                        field_map, full_field, filter_exp.op, filter_exp.value
                    ),
                )
            )

        # Build a MultiComp if field maps to several values
        if len(map_result) > 1:
            filter_exp = MultiComp(
                ExpOp.OR,
                [_create_comp(field, op, value) for field, op, value in map_result],
            )
        elif len(map_result) == 1:  # it maps to a single value
            mapping = map_result.pop()
            _logger.debug("mapping = %s", mapping)
            field = mapping[0]
            filter_exp.field = _trim_ocsf_event_field(field)
            filter_exp.op = mapping[1]
            filter_exp.value = mapping[2]
        else:  # pass-through
            pass
        # TODO: for RefComparison, map the attribute in value (may not be possible here)

    elif isinstance(filter_exp, BoolExp):
        # recursively map boolean expressions
        filter_exp = BoolExp(
            _map_filter_exp(
                native_projection_field,
                ocsf_projection_field,
                filter_exp.lhs,
                field_map,
            ),
            filter_exp.op,
            _map_filter_exp(
                native_projection_field,
                ocsf_projection_field,
                filter_exp.rhs,
                field_map,
            ),
        )
    elif isinstance(filter_exp, MultiComp):
        # normally, this should be unreachable
        # if this becomes a valid case, we need to change
        # the definition of MultiComp to accept a MultiComp
        # in addition to Comparisons in its `comps` list
        filter_exp = MultiComp(
            filter_exp.op,
            [
                _map_filter_exp(
                    native_projection_field, ocsf_projection_field, x, field_map
                )
                for x in filter_exp.comps
            ],
        )
    return filter_exp


@typechecked
def _add_reference_branches_for_filter(graph: IRGraph, filter_node: Filter):
    if filter_node not in graph:
        raise IRGraphMissingNode("Internal error: filter node expected")
    else:
        for refvalue in filter_node.get_references():
            r = graph.add_node(Reference(refvalue.reference))
            p = graph.add_node(ProjectAttrs(refvalue.attributes), r)
            graph.add_edge(p, filter_node)


class _KestrelT(Transformer):
    def __init__(
        self,
        irgraph: IRGraph,
        field_map,
        type_map,
        entity_entity_relation_table,
        entity_event_relation_table,
        entity_identifier_map,
        token_prefix="",
        default_sort_order=DEFAULT_SORT_ORDER,
    ):
        # token_prefix is the modification by Lark when using `merge_transformers()`
        self.irgraph = irgraph  # for reference use, do not modify
        self.default_sort_order = default_sort_order
        self.token_prefix = token_prefix
        self.type_map = type_map
        self.field_map = field_map
        self.entity_identifier_map = entity_identifier_map
        self.variable_map = {}  # To cache var type info
        self.entity_entity_relation_table = entity_entity_relation_table
        self.entity_event_relation_table = entity_event_relation_table
        super().__init__()

    def start(self, args):
        return reduce(compose, args, IRGraph())

    def statement(self, args):
        return args[0]

    def assignment(self, args):
        # TODO: move the var+var into expression in Lark
        graph, root = args[1]
        entity_type, native_type = self._get_type_from_predecessors(graph, root)
        variable_node = Variable(args[0].value, entity_type, native_type)
        self.variable_map[args[0].value] = (entity_type, native_type)
        graph.add_node(variable_node, root)
        return graph

    def expression(self, args):
        # TODO: add more clauses than WHERE and ATTR
        # TODO: think about order of clauses when turning into nodes
        graph = IRGraph()
        reference = graph.add_node(args[0])
        root = reference
        if len(args) > 1:
            for clause in args[1:]:
                graph.add_node(clause, root)
                root = clause
                if isinstance(clause, Filter):
                    # this is where_clause
                    _add_reference_branches_for_filter(graph, clause)
        return graph, root

    def vtrans(self, args):
        if len(args) == 1:
            return Reference(args[0].value)
        else:
            # TODO: transformer support
            ...

    def new(self, args):
        # TODO: use entity type

        graph = IRGraph()
        if len(args) == 1:
            # Try to get entity type from first entity
            entity_type = None
            data = args[0]
        else:
            entity_type = args[0].value
            data = args[1]
        data_node = Construct(data, entity_type)
        graph.add_node(data_node)
        return graph, data_node

    def var_data(self, args):
        if isinstance(args[0], Token):
            # TODO
            ...
        else:
            v = args[0]
        return v

    def json_objs(self, args):
        return args

    def json_obj(self, args):
        return dict(args)

    def json_pair(self, args):
        v = args[0].value
        if "ESCAPED_STRING" in args[0].type:
            v = unescape_quoted_string(v)
        return v, args[1]

    def json_value(self, args):
        v = args[0].value
        if args[0].type == self.token_prefix + "ESCAPED_STRING":
            v = unescape_quoted_string(v)
        elif args[0].type == self.token_prefix + "NUMBER":
            v = float(v) if "." in v else int(v)
        return v

    def variables(self, args):
        return [Reference(arg.value) for arg in args]

    def get(self, args):
        graph = IRGraph()
        native_projection_field = args[0].value
        ocsf_projection_field = translate_entity_projection_to_ocsf(
            self.field_map, native_projection_field
        )

        # prepare Filter node
        filter_node = args[2]
        filter_node.exp = _map_filter_exp(
            native_projection_field,
            ocsf_projection_field,
            filter_node.exp,
            self.field_map,
        )

        # add basic Source and Filter nodes
        source_node = graph.add_node(args[1])
        filter_node = graph.add_node(filter_node, source_node)

        # add reference nodes if used in Filter
        _add_reference_branches_for_filter(graph, filter_node)

        projection_node = graph.add_node(
            ProjectEntity(ocsf_projection_field, native_projection_field), filter_node
        )
        root = projection_node
        if len(args) > 3:
            for arg in args[3:]:
                if isinstance(arg, TimeRange):
                    filter_node.timerange = arg
                elif isinstance(arg, Limit):
                    root = graph.add_node(arg, projection_node)
        return graph, root

    def find(self, args):
        return_entity_type = args[0].value
        relation = args[1].value
        if_reverse, input_var_ref = (
            (True, Reference(args[3].value))
            if hasattr(args[2], "type")
            and args[2].type == self.token_prefix + "REVERSED"
            else (False, Reference(args[2].value))
        )
        filter_node = Filter()
        if len(args) > 3:
            for arg in args[3:]:
                if isinstance(arg, Filter):
                    filter_node = arg
                if isinstance(arg, TimeRange):
                    filter_node.timerange = arg
                elif isinstance(arg, Limit):
                    limit_node = arg

    def apply(self, args):
        scheme, analytic_name = args[0]
        refvar = args[1][0]  # TODO - this is a list of refs?
        params = args[2] if len(args) > 2 else {}
        vds = AnalyticsInterface(interface=scheme)
        analytic = Analytic(name=analytic_name, params=params)
        _logger.debug("apply: analytic: %s", analytic)
        graph = IRGraph()
        graph.add_node(refvar)
        graph.add_node(analytic, refvar)
        graph.add_node(vds)
        graph.add_edge(vds, analytic)
        entity_type, native_type = self.variable_map.get(refvar.name)
        variable_node = Variable(refvar.name, entity_type, native_type)
        graph.add_node(variable_node, analytic)
        return graph

    def where_clause(self, args):
        exp = args[0]
        return Filter(exp)

    def attr_clause(self, args):
        attrs = args[0].split(",")
        attrs = tuple(attr.strip() for attr in attrs)
        return ProjectAttrs(attrs)

    def sort_clause(self, args):
        # args[0] is Token('BY', 'BY')
        return Sort(*args[1:])

    def expression_or(self, args):
        return BoolExp(args[0], ExpOp.OR, args[1])

    def expression_and(self, args):
        return BoolExp(args[0], ExpOp.AND, args[1])

    def comparison_std(self, args):
        """Emit a Comparison object for a Filter"""
        field = args[0].value
        op = args[1]
        value = args[2]
        comp = _create_comp(field, op, value)
        return comp

    def args(self, args):
        return dict(args)

    def arg_kv_pair(self, args):
        name = args[0].value
        if isinstance(args[1], ReferenceValue):
            value = args[1].reference
        else:
            value = args[1]  # Should be int or float?
        return (name, value)

    def op(self, args):
        """Convert operator token to a plain string"""
        return " ".join([arg.upper() for arg in args])

    def op_keyword(self, args):
        """Convert operator token to a plain string"""
        return args[0].value

    # Literals
    def advanced_string(self, args):
        value = _unescape_quoted_string(args[0].value)
        return value

    def reference_or_simple_string(self, args):
        vname = args[0].value
        attr = args[1].value if len(args) > 1 else None
        return ReferenceValue(vname, (attr,))

    def number(self, args):
        v = args[0].value
        try:
            return int(v)
        except ValueError:
            return float(v)

    def value(self, args):
        return args[0]

    def literal_list(self, args):
        return args

    def literal(self, args):
        return args[0]

    def datasource(self, args):
        return DataSource(args[0].value)

    def analytics_uri(self, args):
        scheme, _, analytic = args[0].value.partition("://")
        _logger.debug("analytics_uri: %s %s", scheme, analytic)
        return scheme, analytic

    # Timespans
    def timespan_relative(self, args):
        num = int(args[0])
        unit = args[1]
        if unit == "DAY":
            delta = timedelta(days=num)
        elif unit == "HOUR":
            delta = timedelta(hours=num)
        elif unit == "MINUTE":
            delta = timedelta(minutes=num)
        elif unit == "SECOND":
            delta = timedelta(seconds=num)
        stop = datetime.now(timezone.utc)
        start = stop - delta
        return TimeRange(start, stop)

    def timespan_absolute(self, args):
        start = to_datetime(args[0])
        stop = to_datetime(args[1])
        return TimeRange(start, stop)

    def day(self, _args):
        return "DAY"

    def hour(self, _args):
        return "HOUR"

    def minute(self, _args):
        return "MINUTE"

    def second(self, _args):
        return "SECOND"

    def timestamp(self, args):
        return args[0]

    # Limit
    def limit_clause(self, args):
        n = int(args[0])
        return Limit(n)

    def offset_clause(self, args):
        n = int(args[0])
        return Offset(n)

    def disp(self, args):
        graph, root = args[0]
        _logger.debug("disp: root = %s", root)
        if isinstance(root, ProjectAttrs):
            # Map attrs to OCSF
            entity_type, native_type = self._get_type_from_predecessors(graph, root)
            _logger.debug(
                "Map %s attrs to OCSF %s in %s", native_type, entity_type, root
            )
            root.attrs = translate_attributes_projection_to_ocsf(
                self.field_map, native_type, entity_type, root.attrs
            )
        graph.add_node(Return(), root)
        return graph

    def explain(self, args):
        graph = IRGraph()
        reference = graph.add_node(Reference(args[0].value))
        explain = graph.add_node(Explain(), reference)
        graph.add_node(Return(), explain)
        return graph

    def _get_type_from_predecessors(self, graph: IRGraph, root: Instruction):
        stack = [root]
        native_type = None
        entity_type = None
        while stack and not all((native_type, entity_type)):
            curr = stack.pop()
            _logger.debug("_get_type: curr = %s", curr)
            stack.extend(graph.predecessors(curr))
            if isinstance(curr, ProjectEntity):
                native_type = curr.native_field
                entity_type = self.type_map.get(curr.ocsf_field, curr.ocsf_field)
            elif isinstance(curr, Variable):
                native_type = curr.native_type
                entity_type = curr.entity_type
            elif isinstance(curr, Construct):
                native_type = curr.entity_type
                entity_type = self.type_map.get(native_type, native_type)
        return entity_type, native_type
