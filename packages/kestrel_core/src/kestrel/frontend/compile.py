# Lark Transformer

import logging
from datetime import datetime, timedelta, timezone
from itertools import chain
from typing import List, Union

from dateutil.parser import parse as to_datetime
from kestrel.exceptions import (
    DuplicatedRelationMapping,
    InvalidComparison,
    MissingEntityIdentifierInConfig,
    UnsupportedObjectRelation,
)
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
from kestrel.ir.graph import IRGraph
from kestrel.ir.instructions import (
    Analytic,
    AnalyticsInterface,
    Construct,
    DataSource,
    Explain,
    Filter,
    Information,
    Instruction,
    Limit,
    Offset,
    ProjectAttrs,
    ProjectEntity,
    Return,
    Sort,
    Variable,
)
from kestrel.mapping.data_model import (
    translate_attributes_projection_to_ocsf,
    translate_comparison_to_ocsf,
    translate_entity_projection_to_ocsf,
)
from kestrel.utils import unescape_quoted_string
from lark import Token, Transformer
from pandas import DataFrame
from typeguard import typechecked

_logger = logging.getLogger(__name__)


DEFAULT_VARIABLE = "_"
DEFAULT_SORT_ORDER = "DESC"


@typechecked
def _unescape_quoted_string(s: str) -> str:
    if s.startswith("r"):
        return s[2:-1]
    else:
        return s[1:-1].encode("utf-8").decode("unicode_escape")


@typechecked
def _trim_ocsf_base_field(field: str, only_trim_event: bool) -> str:
    """Remove event name (as prefix) if the field starts with an event"""
    items = field.split(".")
    if (not only_trim_event) or (
        only_trim_event
        and (
            items[0].endswith("_event")
            or items[0].endswith("_activity")
            or items[0] == "event"
        )
    ):
        items = items[1:]
    field = ".".join(items)
    return field


@typechecked
def _create_comp(
    field: str,
    op_value: str,
    value: Union[str, int, float, List, ReferenceValue],
    only_trim_event: bool,
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
        comp = compf(
            [_trim_ocsf_base_field(field, only_trim_event)], op(op_value), value
        )
    else:
        comp = compf(_trim_ocsf_base_field(field, only_trim_event), op(op_value), value)

    return comp


@typechecked
def _map_filter_exp(
    native_projection_field: str,
    ocsf_projection_field: str,
    filter_exp: FExpression,
    field_map: dict,
    only_trim_event: bool,
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

        map_result = translate_comparison_to_ocsf(
            field_map, field, filter_exp.op, filter_exp.value
        )
        # there is a case that `field` omits the return entity (prefix)
        # this is only alloed when it refers to the return entity
        # add mapping for those cases
        for full_field in (
            f"{native_projection_field}:{field}",
            f"{native_projection_field}.{field}",
        ):
            for triple in translate_comparison_to_ocsf(
                field_map, full_field, filter_exp.op, filter_exp.value
            ):
                if (
                    triple[0].startswith(ocsf_projection_field + ".")
                    and triple not in map_result
                ):
                    map_result.append(triple)

        # Build a MultiComp if field maps to several values
        if len(map_result) > 1:
            filter_exp = MultiComp(
                ExpOp.OR,
                [
                    _create_comp(field, op, value, only_trim_event)
                    for field, op, value in map_result
                ],
            )
        elif len(map_result) == 1:  # it maps to a single value
            mapping = map_result.pop()
            _logger.debug("mapping = %s", mapping)
            field = mapping[0]
            filter_exp.field = _trim_ocsf_base_field(field, only_trim_event)
            filter_exp.op = mapping[1]
            filter_exp.value = mapping[2]
        else:  # pass-through
            if field.startswith(ocsf_projection_field + "."):
                _field = field
            else:
                _field = ocsf_projection_field + "." + field
            _field = _trim_ocsf_base_field(_field, only_trim_event)
            if _field != field:
                if hasattr(filter_exp, "field"):
                    filter_exp.field = _field
                elif hasattr(filter_exp, "fields"):
                    filter_exp.fields[0] = _field

        # TODO: for RefComparison, map the attribute in value (may not be possible here)

    elif isinstance(filter_exp, BoolExp):
        # recursively map boolean expressions
        filter_exp = BoolExp(
            _map_filter_exp(
                native_projection_field,
                ocsf_projection_field,
                filter_exp.lhs,
                field_map,
                only_trim_event,
            ),
            filter_exp.op,
            _map_filter_exp(
                native_projection_field,
                ocsf_projection_field,
                filter_exp.rhs,
                field_map,
                only_trim_event,
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
                    native_projection_field,
                    ocsf_projection_field,
                    x,
                    field_map,
                    only_trim_event,
                )
                for x in filter_exp.comps
            ],
        )
    return filter_exp


@typechecked
def _get_entity_event_relation_projection(
    table: DataFrame,
    output_type: str,
    relation: str,
) -> str:
    t1 = table[table["OutputType"] == output_type]
    if t1.empty:
        raise UnsupportedObjectRelation("event", output_type)
    else:
        t2 = t1[t1["Relation"] == relation]
        if t2.empty:
            supported_relations = t1["Relation"].tolist()
            raise UnsupportedObjectRelation(
                "event",
                relation,
                output_type,
                f"Supported: {supported_relations}",
            )
        elif t2.shape[0] > 1:
            raise DuplicatedRelationMapping(
                "event", relation, output_type, output_projections, t2
            )
        else:
            return t2["OutputProjection"].iloc[0]


@typechecked
def _get_entity_entity_relation_specifier_projection(
    table: DataFrame,
    input_type: str,
    output_type: str,
    relation: str,
) -> (str, str):
    t1 = table[
        (table["OutputType"] == output_type) & (table["InputType"] == input_type)
    ]
    if t1.empty:
        raise UnsupportedObjectRelation(input_type, output_type)
    else:
        t2 = t1[t1["Relation"] == relation]
        if t2.empty:
            supported_relations = t1["Relation"].tolist()
            raise UnsupportedObjectRelation(
                input_type,
                relation,
                output_type,
                f"Supported: {supported_relations}",
            )
        elif t2.shape[0] > 1:
            raise DuplicatedRelationMapping(input_type, relation, output_type, t2)
        else:
            return t2["InputSpecifier"].iloc[0], t2["OutputProjection"].iloc[0]


@typechecked
def _create_filter_for_find(
    entity_identifier_map: dict,
    input_var: Variable,
    input_specifier: str,
):
    if input_var.entity_type not in entity_identifier_map:
        raise MissingEntityIdentifierInConfig(
            input_var.entity_type, entity_identifier_map
        )
    else:
        identifiers = entity_identifier_map[input_var.entity_type]
        ref_val = ReferenceValue(input_var.name, tuple(identifiers))
        comp_fields = [input_specifier + "." + x for x in identifiers]
        return Filter(RefComparison(comp_fields, ListOp.IN, ref_val))


@typechecked
class _KestrelT(Transformer):
    """Kestrel Lark Transformer

    Returns for different methods:
        - statement: [Return]
        - assignment: [Return]
        - command_no_result: [Return]
        - expression: (Instruction(root), str(entity_type), str(native_type))
        - command_with_result: (Instruction(root), str(entity_type), str(native_type))
    """

    def __init__(
        self,
        irgraph: IRGraph,
        field_map: dict,
        type_map: dict,
        entity_entity_relation_table: DataFrame,
        entity_event_relation_table: DataFrame,
        entity_identifier_map: dict,
        token_prefix: str = "",
        default_sort_order: str = DEFAULT_SORT_ORDER,
    ):
        # token_prefix is the modification by Lark when using `merge_transformers()`
        self.irgraph = irgraph
        self.default_sort_order = default_sort_order
        self.token_prefix = token_prefix
        self.type_map = type_map
        self.field_map = field_map
        self.entity_identifier_map = entity_identifier_map
        self.entity_entity_relation_table = entity_entity_relation_table
        self.entity_event_relation_table = entity_event_relation_table
        super().__init__()

    def start(self, args) -> List[Return]:
        """Parse/transform statement, and update IRGraph

        All statements should return a list of Return.
        Merge and return all of them.

        self.irgraph is updated in each statement transformer. This is required
        to search for a variable in previous IRGraph and graph generated in
        this code block.
        """
        return list(chain(*args))

    def statement(self, args) -> List[Return]:
        return args[0]

    def assignment(self, args) -> List[Return]:
        # TODO: x = y + z
        root, entity_type, native_type = args[1]
        variable = Variable(args[0].value, entity_type, native_type)
        self.irgraph.add_node(variable, root)
        return []

    def expression(self, args) -> (Instruction, str, str):
        # TODO: add more clauses than WHERE and ATTR
        # TODO: think about order of clauses when turning into nodes
        variable = args[0]
        root = variable
        if len(args) > 1:
            for clause in args[1:]:
                if isinstance(clause, Filter):
                    clause.exp = _map_filter_exp(
                        variable.native_type,
                        variable.native_type,
                        clause.exp,
                        self.field_map,
                        False,
                    )
                root = self.irgraph.add_node(clause, root)
        return root, variable.entity_type, variable.native_type

    def vtrans(self, args) -> Variable:
        if len(args) == 1:
            return self.irgraph.get_variable(args[0].value)
        else:
            # TODO: transformer support
            ...

    def new(self, args) -> (Instruction, str, str):
        native_type = None
        if len(args) == 2:
            native_type = args[0].value
            data = args[1]
        data_node = self.irgraph.add_node(Construct(data, native_type))
        if not native_type:
            raise NotImplementedError("Infer type from NEW")
        entity_type = self.type_map.get(native_type, native_type)
        return data_node, entity_type, native_type

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

    def variables(self, args) -> List[Variable]:
        return [self.irgraph.get_variable(arg.value) for arg in args]

    def get(self, args) -> (Instruction, str, str):
        # 0. get information of projection and return
        native_projection_field = args[0].value
        ocsf_projection_field = translate_entity_projection_to_ocsf(
            self.field_map, native_projection_field
        )
        output_type = self.type_map.get(ocsf_projection_field, ocsf_projection_field)

        # 1. process DataSource
        source_node = self.irgraph.add_node(args[1])

        # 2. process Filter
        filter_node = args[2]
        filter_node.exp = _map_filter_exp(
            native_projection_field,
            ocsf_projection_field,
            filter_node.exp,
            self.field_map,
            True,
        )
        filter_node = self.irgraph.add_node(filter_node, source_node)

        # 3. process ProjectEntity
        projection_node = self.irgraph.add_node(
            ProjectEntity(ocsf_projection_field, native_projection_field), filter_node
        )
        root = projection_node

        # 4. process additional instructions/nodes
        if len(args) > 3:
            for arg in args[3:]:
                if isinstance(arg, TimeRange):
                    filter_node.timerange = arg
                elif isinstance(arg, Limit):
                    root = self.irgraph.add_node(arg, projection_node)

        return root, output_type, native_projection_field

    def find(self, args) -> (Instruction, str, str):
        output_var_type = args[0].value
        relation = args[1].value.upper()
        if_reverse, input_var_name = (
            (True, args[3].value)
            if hasattr(args[2], "type")
            and args[2].type == self.token_prefix + "REVERSED"
            else (False, args[2].value)
        )
        input_var = self.irgraph.get_variable(input_var_name)
        input_var_type = input_var.entity_type

        if input_var_type == "event":  # event-to-entity relation
            output_projection = _get_entity_event_relation_projection(
                self.entity_event_relation_table,
                output_var_type,
                relation,
            )
            filter_node = self.irgraph.add_node(Filter(), input_var)
            root = self.irgraph.add_node(
                ProjectEntity(output_projection, output_projection), filter_node
            )
        elif output_var_type == "event":  # entity-to-event relation
            if not if_reverse:  # this relation always require `BY`
                raise UnsupportedObjectRelation(f'Missing "BY" after "{relation}"')
            input_specifier = _get_entity_event_relation_projection(
                self.entity_event_relation_table,
                input_var_type,
                relation,
            )
            filter_node = _create_filter_for_find(
                self.entity_identifier_map,
                input_var,
                input_specifier,
            )
            ds = self.irgraph.find_datasource_of_node(input_var)
            filter_node = self.irgraph.add_node(filter_node, ds)

            # still need projection for translation/mapping to take place
            root = self.irgraph.add_node(
                ProjectEntity(output_var_type, output_var_type), filter_node
            )
        else:  # entity-to-entity relation
            lookup_input_type, lookup_output_type = (
                (output_var_type, input_var_type)
                if if_reverse
                else (input_var_type, output_var_type)
            )
            input_specifier, output_projection = (
                _get_entity_entity_relation_specifier_projection(
                    self.entity_entity_relation_table,
                    lookup_input_type,
                    lookup_output_type,
                    relation,
                )
            )
            input_specifier, output_projection = (
                (output_projection, input_specifier)
                if if_reverse
                else (input_specifier, output_projection)
            )
            filter_node = _create_filter_for_find(
                self.entity_identifier_map,
                input_var,
                input_specifier,
            )
            ds = self.irgraph.find_datasource_of_node(input_var)
            filter_node = self.irgraph.add_node(filter_node, ds)
            root = self.irgraph.add_node(
                ProjectEntity(output_projection, output_projection), filter_node
            )

        if len(args) > 3:
            for arg in args[3:]:
                if isinstance(arg, Filter):
                    # merge the user-specified filter (where clause)
                    filter_node.exp = BoolExp(filter_node.exp, ExpOp.AND, arg.exp)
                if isinstance(arg, TimeRange):
                    # set user-specified time range
                    filter_node.timerange = arg
                elif isinstance(arg, Limit):
                    root = self.irgraph.add_node(arg, root)

        return root, output_var_type, output_var_type

    def apply(self, args) -> List[Return]:
        scheme, analytic_name = args[0]
        if len(args[1]) > 1:
            raise NotImplementedError("Apply on multiple variables")
        else:
            refvar = args[1][0]
        params = args[2] if len(args) > 2 else {}
        vds = AnalyticsInterface(interface=scheme)
        analytic = Analytic(name=analytic_name, params=params)
        _logger.debug("apply: analytic: %s", analytic)

        output_var = Variable(refvar.name, refvar.entity_type, refvar.native_type)

        self.irgraph.add_node(analytic, refvar)
        self.irgraph.add_node(vds)
        self.irgraph.add_edge(vds, analytic)
        self.irgraph.add_node(output_var, analytic)
        return []

    def where_clause(self, args) -> Filter:
        exp = args[0]
        return Filter(exp)

    def attr_clause(self, args) -> ProjectAttrs:
        attrs = args[0].split(",")
        attrs = tuple(attr.strip() for attr in attrs)
        return ProjectAttrs(attrs)

    def sort_clause(self, args) -> Sort:
        # args[0] is Token('BY', 'BY')
        return Sort(*args[1:])

    def expression_or(self, args) -> BoolExp:
        return BoolExp(args[0], ExpOp.OR, args[1])

    def expression_and(self, args) -> BoolExp:
        return BoolExp(args[0], ExpOp.AND, args[1])

    def comparison_std(self, args) -> FComparison:
        """Emit a Comparison object for a Filter"""
        field = args[0].value
        op = args[1]
        value = args[2]
        return _create_comp(field, op, value, True)

    def args(self, args) -> dict:
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
    def advanced_string(self, args) -> str:
        value = _unescape_quoted_string(args[0].value)
        return value

    def reference_or_simple_string(self, args) -> ReferenceValue:
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

    def datasource(self, args) -> DataSource:
        return DataSource(args[0].value)

    def analytics_uri(self, args) -> (str, str):
        scheme, _, analytic = args[0].value.partition("://")
        _logger.debug("analytics_uri: %s %s", scheme, analytic)
        return scheme, analytic

    # Timespans
    def timespan_relative(self, args) -> TimeRange:
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

    def timespan_absolute(self, args) -> TimeRange:
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

    def timestamp(self, args) -> str:
        return args[0]

    # Limit
    def limit_clause(self, args) -> Limit:
        n = int(args[0])
        return Limit(n)

    def offset_clause(self, args) -> Offset:
        n = int(args[0])
        return Offset(n)

    def disp(self, args) -> List[Return]:
        root, entity_type, native_type = args[0]
        _logger.debug("disp: root = %s", root)
        if isinstance(root, ProjectAttrs):
            root.attrs = translate_attributes_projection_to_ocsf(
                self.field_map, native_type, entity_type, root.attrs
            )
        ret = self.irgraph.add_node(Return(), root)
        return [ret]

    def explain(self, args) -> List[Return]:
        variable = self.irgraph.get_variable(args[0].value)
        explain = self.irgraph.add_node(Explain(), variable)
        ret = self.irgraph.add_node(Return(), explain)
        return [ret]

    def info(self, args) -> List[Return]:
        variable = self.irgraph.get_variable(args[0].value)
        info = self.irgraph.add_node(Information(), variable)
        ret = self.irgraph.add_node(Return(), info)
        return [ret]
