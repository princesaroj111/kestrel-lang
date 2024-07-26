import os
from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest
from pandas import DataFrame, read_csv

from kestrel.config import load_kestrel_config
from kestrel.frontend.parser import parse_kestrel_and_update_irgraph
from kestrel.ir.filter import AbsoluteTrue, RefComparison, ReferenceValue
from kestrel.ir.graph import IRGraph
from kestrel.ir.instructions import (Construct, DataSource, Explain, Filter,
                                     Limit, Offset, ProjectAttrs,
                                     ProjectEntity, Reference, Return,
                                     SerializableDataFrame, Sort, Variable)


@pytest.fixture
def process_creation_events():
    # return a two-node graph:
    #   - Construct: table from logs_ocsf_process_creation.csv
    #   - Variable es: events pointing to the Construct
    graph = IRGraph()
    parse_kestrel_and_update_irgraph("es = NEW event [ {'id': 1} ]", graph, {})
    data_node = graph.get_nodes_by_type(Construct)[0]
    test_dir = os.path.dirname(os.path.abspath(__file__))
    data_node.data = SerializableDataFrame(read_csv(os.path.join(test_dir, "logs_ocsf_process_creation.csv")))
    return graph


@pytest.fixture
def kestrel_config():
    return load_kestrel_config()


@pytest.mark.parametrize(
    "stmt", [
        "x = GET thing FROM if://ds WHERE foo = 'bar'",
        "x = GET thing FROM if://ds WHERE foo > 1.5",
        r"x = GET thing FROM if://ds WHERE foo = r'C:\TMP'",
        "x = GET thing FROM if://ds WHERE foo = 'bar' OR baz != 42",
        "x = GET thing FROM if://ds WHERE foo = 'bar' AND baz IN (1, 2, 3)",
        "x = GET thing FROM if://ds WHERE foo = 'bar' AND baz IN (1)",
        "x = GET thing FROM if://ds WHERE foo = 'bar' AND baz IN (1) LAST 3 DAYS",
    ]
)
def test_parser_get_statements(stmt):
    """
    This test isn't meant to be comprehensive, but checks basic transformer functionality.

    This will need to be updated as we build out the new Transformer
    """

    graph = IRGraph()
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    assert len(graph) == 4
    assert len(graph.get_nodes_by_type(Variable)) == 1
    assert len(graph.get_nodes_by_type(ProjectEntity)) == 1
    assert len(graph.get_nodes_by_type(DataSource)) == 1
    assert len(graph.get_nodes_by_type(Filter)) == 1

    # Ensure result is serializable
    _ = graph.to_json()


def test_parser_get_timespan_relative():
    stmt = "x = GET url FROM if://ds WHERE url = 'http://example.com/' LAST 5h"
    graph = IRGraph()
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    filt_list = graph.get_nodes_by_type(Filter)
    assert len(filt_list) == 1
    filt = filt_list[0]
    delta = filt.timerange.stop - filt.timerange.start
    assert delta == timedelta(hours=5)


def test_parser_get_timespan_absolute():
    stmt = ("x = GET url FROM if://ds WHERE url = 'http://example.com/'"
            " START '2023-11-29T00:00:00Z' STOP '2023-11-29T05:00:00Z'")
    graph = IRGraph()
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    filt_list = graph.get_nodes_by_type(Filter)
    assert len(filt_list) == 1
    filt = filt_list[0]
    delta = filt.timerange.stop - filt.timerange.start
    assert delta == timedelta(hours=5)
    assert filt.timerange.start == datetime(2023, 11, 29, 0, 0, tzinfo=timezone.utc)
    assert filt.timerange.stop == datetime(2023, 11, 29, 5, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "stmt, expected", [
        ("x = GET url FROM if://ds WHERE url = 'http://example.com/' LIMIT 1", 1),
        ("x = GET url FROM if://ds WHERE url = 'http://example.com/' LAST 3d LIMIT 2", 2),
        (("x = GET url FROM if://ds WHERE url = 'http://example.com/'"
          " START '2023-11-29T00:00:00Z' STOP '2023-11-29T05:00:00Z' LIMIT 3"), 3),
    ]
)
def test_parser_get_with_limit(stmt, expected):
    graph = IRGraph()
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    limits = graph.get_nodes_by_type(Limit)
    assert len(limits) == 1
    limit = limits[0]
    assert limit.num == expected


def get_parsed_filter_exp(stmt):
    graph = IRGraph()
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    filter_node = graph.get_nodes_by_type(Filter).pop()
    return filter_node.exp


def test_parser_mapping_single_comparison():
    # test when entity name is not included in the attributes
    stmt = "x = GET process FROM if://ds WHERE binary_ref.name = 'foo'"
    parse_filter = get_parsed_filter_exp(stmt)
    assert parse_filter.field == 'process.file.name'
    # another test
    stmt = "x = GET ipv4-addr FROM if://ds WHERE value = '192.168.22.3'"
    parse_filter = get_parsed_filter_exp(stmt)
    assert parse_filter.field == 'device.ip'

    # this is a special case in parser logic
    # if the field already has entity prefix, do not filter it with entity type
    # since extended graph (from non-return entity) can be put here
    stmt = "x = GET process FROM if://ds WHERE process:binary_ref.name = 'foo'"
    fields = set([x.field for x in get_parsed_filter_exp(stmt).comps])
    assert fields == set(('process.file.name', 'actor.process.file.name'))


def test_parser_stix_mapping_network_traffic():
    stmt = "x = GET network-traffic FROM if://ds WHERE src_ref.value = '192.168.22.3'"
    parse_filter = get_parsed_filter_exp(stmt)
    assert parse_filter.field == 'src_endpoint.ip'


def test_parser_mapping_multiple_comparison_to_multiple_values():
    stmt = "x = GET process FROM if://ds WHERE binary_ref.name = 'foo' "\
        "OR name = 'bam' AND parent_ref.name = 'boom'"
    parse_filter = get_parsed_filter_exp(stmt)
    field1 = parse_filter.lhs.field
    assert field1 == 'process.file.name'
    field2 = parse_filter.rhs.lhs.field
    assert field2 == 'process.name'  # 'process.name'
    field3 = parse_filter.rhs.rhs.field
    assert field3 == "process.parent_process.name"


def test_parser_new_json():
    stmt = """
proclist = NEW process [ {"name": "cmd.exe", "pid": 123}
                       , {"name": "explorer.exe", "pid": 99}
                       , {"name": "firefox.exe", "pid": 201}
                       , {"name": "chrome.exe", "pid": 205}
                       ]
"""
    graph = IRGraph()
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    cs = graph.get_nodes_by_type(Construct)
    assert len(cs) == 1
    construct = cs[0]
    df = DataFrame([ {"name": "cmd.exe", "pid": 123}
         , {"name": "explorer.exe", "pid": 99}
         , {"name": "firefox.exe", "pid": 201}
         , {"name": "chrome.exe", "pid": 205}
         ])
    assert df.equals(construct.data)
    vs = graph.get_variables()
    assert len(vs) == 1
    assert vs[0].name == "proclist"


@pytest.mark.parametrize(
    "stmt, node_cnt", [
        ("x = y WHERE foo = 'bar'", 4),
        ("x = y WHERE foo > 1.5", 4),
        (r"x = y WHERE foo = r'C:\TMP'", 4),
        ("x = y WHERE foo = 'bar' OR baz != 42", 4),
        ("x = y WHERE foo = 'bar' AND baz IN (1, 2, 3)", 4),
        ("x = y WHERE foo = 'bar' AND baz IN (1)", 4),
        ("x = y WHERE foo = 'bar' SORT BY foo ASC LIMIT 3", 6),
        ("x = y WHERE foo = 'bar' SORT BY foo ASC LIMIT 3 OFFSET 9", 7),
    ]
)
def test_parser_expression(stmt, node_cnt):
    """
    This test isn't meant to be comprehensive, but checks basic transformer functionality.

    This will need to be updated as we build out the new Transformer
    """

    graph = IRGraph()
    parse_kestrel_and_update_irgraph('y = NEW process [ {"asdf": "abc.exe"} ]', graph, {})
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    assert len(graph) == node_cnt
    assert len(graph.get_nodes_by_type(Variable)) == 2
    assert len(graph.get_nodes_by_type(Filter)) == 1
    assert len(graph.get_nodes_by_type(Sort)) in (0, 1)
    assert len(graph.get_nodes_by_type(Limit)) in (0, 1)
    assert len(graph.get_nodes_by_type(Offset)) in (0, 1)


def test_three_statements_in_a_line():
    stmt = """
proclist = NEW process [ {"name": "cmd.exe", "pid": 123}
                       , {"name": "explorer.exe", "pid": 99}
                       , {"name": "firefox.exe", "pid": 201}
                       , {"name": "chrome.exe", "pid": 205}
                       ]
browsers = proclist WHERE name = 'firefox.exe' OR name = 'chrome.exe'
DISP browsers ATTR name, pid
"""
    graph = IRGraph()
    rets = parse_kestrel_and_update_irgraph(stmt, graph, {})
    assert len(graph) == 6
    c = graph.get_nodes_by_type(Construct)[0]
    assert {"proclist", "browsers"} == {v.name for v in graph.get_variables()}
    proclist = graph.get_variable("proclist")
    browsers = graph.get_variable("browsers")
    proj = graph.get_nodes_by_type(ProjectAttrs)[0]
    assert proj.attrs == ('name', 'pid')
    ft = graph.get_nodes_by_type(Filter)[0]
    assert ft.exp.to_dict() == {"lhs": {"field": "name", "op": "=", "value": "firefox.exe"}, "op": "OR", "rhs": {"field": "name", "op": "=", "value": "chrome.exe"}}
    assert len(graph.edges) == 5
    assert (c, proclist) in graph.edges
    assert (proclist, ft) in graph.edges
    assert (ft, browsers) in graph.edges
    assert (browsers, proj) in graph.edges
    assert (proj, rets[0]) in graph.edges


@pytest.mark.parametrize(
    "stmt, node_cnt, expected", [
        ("x = y WHERE foo = y.foo", 5, [ReferenceValue("y", ("foo",))]),
        ("x = y WHERE foo > 1.5", 4, []),
        ("x = y WHERE foo = 'bar' OR baz = y.baz", 5, [ReferenceValue("y", ("baz",))]),
        ("x = y WHERE (foo = 'bar' OR baz = y.baz) AND (fox = y.fox AND bbb = y.bbb)", 7, [ReferenceValue("y", ("baz",)), ReferenceValue("y", ("fox",)), ReferenceValue("y", ("bbb",))]),
        ("x = GET process FROM s://x WHERE foo = y.foo", 7, [ReferenceValue("y", ("foo",))]),
        ("x = GET file FROM s://y WHERE foo > 1.5", 6, []),
        ("x = GET file FROM c://x WHERE foo = 'bar' OR baz = y.baz", 7, [ReferenceValue("y", ("baz",))]),
        ("x = GET user FROM s://x WHERE (foo = 'bar' OR baz = y.baz) AND (fox = y.fox AND bbb = y.bbb)", 9, [ReferenceValue("y", ("baz",)), ReferenceValue("y", ("fox",)), ReferenceValue("y", ("bbb",))]),
    ]
)
def test_reference_branch(stmt, node_cnt, expected):
    graph = IRGraph()
    parse_kestrel_and_update_irgraph('y = NEW process [ {"asdf": "abc.exe"} ]', graph, {})
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    assert len(graph) == node_cnt
    filter_nodes = graph.get_nodes_by_type(Filter)
    assert len(filter_nodes) == 1
    filter_node = filter_nodes[0]
    for rv in expected:
        r = graph.get_variable(rv.reference)
        assert r
        projs = [p for p in graph.successors(r) if isinstance(p, ProjectAttrs) and p.attrs == rv.attributes]
        assert projs and len(projs) == 1
        proj = projs[0]
        assert proj
        assert list(graph.successors(proj)) == [filter_node]


def test_parser_disp_after_new():
    stmt = """
proclist = NEW process [ {"name": "cmd.exe", "pid": 123}
                       , {"name": "explorer.exe", "pid": 99}
                       , {"name": "firefox.exe", "pid": 201}
                       , {"name": "chrome.exe", "pid": 205}
                       ]
DISP proclist ATTR name, pid LIMIT 2 OFFSET 3
"""
    graph = IRGraph()
    rets = parse_kestrel_and_update_irgraph(stmt, graph, {})
    assert len(graph) == 6
    c = graph.get_nodes_by_type(Construct)[0]
    assert {"proclist"} == {v.name for v in graph.get_variables()}
    proclist = graph.get_variable("proclist")
    proj = graph.get_nodes_by_type(ProjectAttrs)[0]
    assert proj.attrs == ('name', 'pid')
    limit = graph.get_nodes_by_type(Limit)[0]
    assert limit.num == 2
    offset = graph.get_nodes_by_type(Offset)[0]
    assert offset.num == 3
    ret = rets[0]
    assert len(graph.edges) == 5
    assert (c, proclist) in graph.edges
    assert (proclist, proj) in graph.edges
    assert (proj, limit) in graph.edges
    assert (limit, offset) in graph.edges
    assert (offset, ret) in graph.edges


def test_parser_explain_alone():
    stmt = 'y = NEW process [ {"asdf": "abc.exe"} ] EXPLAIN y'
    graph = IRGraph()
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    assert len(graph) == 4
    assert len(graph.edges) == 3
    assert Counter(map(type, graph.nodes())) == Counter([Construct, Variable, Explain, Return])


def test_parser_explain_dereferred():
    stmt = """
proclist = NEW process [ {"name": "cmd.exe", "pid": 123}
                       , {"name": "explorer.exe", "pid": 99}
                       , {"name": "firefox.exe", "pid": 201}
                       , {"name": "chrome.exe", "pid": 205}
                       ]
EXPLAIN proclist
"""
    graph = IRGraph()
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    assert len(graph) == 4
    assert len(graph.edges) == 3
    assert Counter(map(type, graph.nodes())) == Counter([Construct, Variable, Explain, Return])


@pytest.mark.parametrize(
    "stmt, entity, ocsf_proj_field, key", [
        ("x = GET registry FROM if://ds WHERE key = 'bar'", "reg_key", "reg_key", "reg_key.path"),
        ("x = GET user-account FROM if://ds WHERE user_id = '123'", "user", "user", "user.uid"),
        ("x = GET host FROM if://ds WHERE name = 'bar'", "endpoint", "device", "device.name"),
        ("x = GET destination FROM if://ds WHERE mac = '22:33:44:55'", "network_endpoint", "dst_endpoint", "dst_endpoint.mac"),
        ("x = GET registry FROM if://ds WHERE key = 'bar'", "reg_key", "reg_key", "reg_key.path"),
        ("x = GET process.parent.user FROM if://ds WHERE name = 'alice'", "user", "process.parent_process.user", "process.parent_process.user.name"),
        ("x = GET process:parent_ref FROM if://ds WHERE command_line = 'cmd.exe -abc'", "process", "process.parent_process", "process.parent_process.cmd_line"),
    ]
)
def test_parser_entity_and_proj_and_field_mapping(stmt, entity, ocsf_proj_field, key):
    graph = IRGraph()
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    assert graph.get_nodes_by_type(Variable)[0].entity_type == entity
    assert graph.get_nodes_by_type(ProjectEntity)[0].ocsf_field == ocsf_proj_field
    assert graph.get_nodes_by_type(Filter)[0].exp.field == key


def test_parser_find_event_to_entity(process_creation_events):
    graph = process_creation_events
    stmt = "procs = FIND process RESPONDED es"
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    assert Counter(map(type, graph.nodes())) == Counter([Construct, Variable, Filter, ProjectEntity, Variable])
    filt = graph.get_nodes_by_type(Filter)[0]
    assert isinstance(filt.exp, AbsoluteTrue)
    projent = graph.get_nodes_by_type(ProjectEntity)[0]
    assert projent.ocsf_field == projent.native_field == "process"


def test_parser_find_entity_to_event(process_creation_events, kestrel_config):
    graph = process_creation_events
    stmt = """
        procs = FIND process RESPONDED es
        eves = FIND event ORIGINATED BY procs
    """
    parse_kestrel_and_update_irgraph(stmt, graph, kestrel_config["entity_identifier"])
    assert Counter(map(type, graph.nodes())) == Counter([Construct, Variable, Filter, ProjectEntity, Variable, ProjectAttrs, Filter, Variable, ProjectEntity])
    projattr = graph.get_nodes_by_type(ProjectAttrs)[0]
    filt = [f for f in graph.get_nodes_by_type(Filter) if not isinstance(f.exp, AbsoluteTrue)][0]
    assert (projattr, filt) in graph.edges
    exp = filt.exp
    assert isinstance(exp, RefComparison)
    assert set(exp.fields) == set(['actor.process.uid', 'actor.process.endpoint.uid'])
    assert exp.value.reference == 'procs'
    assert set(exp.value.attributes) == set(['uid', 'endpoint.uid'])


def test_parser_find_entity_to_entity(process_creation_events, kestrel_config):
    graph = process_creation_events
    stmt = """
        procs = FIND process RESPONDED es
        parents = FIND process CREATED procs
    """
    parse_kestrel_and_update_irgraph(stmt, graph, kestrel_config["entity_identifier"])
    assert Counter(map(type, graph.nodes())) == Counter([Construct, Variable, Filter, ProjectEntity, Variable, ProjectAttrs, Filter, ProjectEntity, Variable])
    projattr = graph.get_nodes_by_type(ProjectAttrs)[0]
    filt = [f for f in graph.get_nodes_by_type(Filter) if not isinstance(f.exp, AbsoluteTrue)][0]
    assert (projattr, filt) in graph.edges
    exp = filt.exp
    assert isinstance(exp, RefComparison)
    assert set(exp.fields) == set(['process.uid', 'process.endpoint.uid'])
    assert exp.value.reference == 'procs'
    assert set(exp.value.attributes) == set(['uid', 'endpoint.uid'])
    parents = graph.get_variable("parents")
    projent = list(graph.predecessors(parents))[0]
    assert projent.ocsf_field == "process.parent_process"
    assert projent.native_field == "process.parent_process"
