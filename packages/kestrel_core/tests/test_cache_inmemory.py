import os
from uuid import uuid4

import pytest
from pandas import DataFrame, read_csv

from kestrel.cache import InMemoryCache
from kestrel.config import load_kestrel_config
from kestrel.frontend.parser import parse_kestrel_and_update_irgraph
from kestrel.ir.graph import IRGraph, IRGraphEvaluable
from kestrel.ir.instructions import Construct, SerializableDataFrame


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


def test_inmemory_cache_set_get_del():
    c = InMemoryCache()
    idx = uuid4()
    df = DataFrame([1, 2, 3])
    c[idx] = df
    assert df.equals(c[idx])
    del c[idx]
    assert idx not in c


def test_inmemory_cache_constructor():
    ids = [uuid4() for i in range(5)]
    df = DataFrame([1, 2, 3])
    c = InMemoryCache({x:df for x in ids})
    for u in ids:
        assert df.equals(c[u])
    for u in ids:
        del c[u]
        assert u not in c


def test_eval_new_filter_disp():
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
    graph = IRGraphEvaluable(graph)
    c = InMemoryCache()
    mapping = c.evaluate_graph(graph, c)

    # check the return is correct
    assert len(rets) == 1
    df = mapping[rets[0].id]
    assert df.to_dict("records") == [ {"name": "firefox.exe", "pid": 201}
                                    , {"name": "chrome.exe", "pid": 205}
                                    ]
    # check whether `proclist` is cached
    proclist = graph.get_variable("proclist")
    assert c[proclist.id].to_dict("records") == [ {"name": "cmd.exe", "pid": 123}
                                                , {"name": "explorer.exe", "pid": 99}
                                                , {"name": "firefox.exe", "pid": 201}
                                                , {"name": "chrome.exe", "pid": 205}
                                                ]
    # check whether `browsers` is cached
    browsers = graph.get_variable("browsers")
    assert c[browsers.id].to_dict("records") == [ {"name": "firefox.exe", "pid": 201}
                                                , {"name": "chrome.exe", "pid": 205}
                                                ]


def test_eval_filter_with_ref():
    stmt = """
proclist = NEW process [ {"name": "cmd.exe", "pid": 123}
                       , {"name": "explorer.exe", "pid": 99}
                       , {"name": "firefox.exe", "pid": 201}
                       , {"name": "chrome.exe", "pid": 205}
                       ]
browsers = proclist WHERE name = 'firefox.exe' OR name = 'chrome.exe'
specials = proclist WHERE pid IN [123, 201]
p2 = proclist WHERE pid = browsers.pid and name = specials.name
DISP p2 ATTR name, pid
"""
    graph = IRGraph()
    rets = parse_kestrel_and_update_irgraph(stmt, graph, {})
    graph = IRGraphEvaluable(graph)
    c = InMemoryCache()
    mapping = c.evaluate_graph(graph, c)

    # check the return is correct
    assert len(rets) == 1
    df = mapping[rets[0].id]
    assert df.to_dict("records") == [ {"name": "firefox.exe", "pid": 201} ]

def test_get_virtual_copy():
    stmt = """
proclist = NEW process [ {"name": "cmd.exe", "pid": 123}
                       , {"name": "explorer.exe", "pid": 99}
                       , {"name": "firefox.exe", "pid": 201}
                       , {"name": "chrome.exe", "pid": 205}
                       ]
browsers = proclist WHERE name = 'firefox.exe' OR name = 'chrome.exe'
"""
    graph = IRGraph()
    parse_kestrel_and_update_irgraph(stmt, graph, {})
    graph = IRGraphEvaluable(graph)
    c = InMemoryCache()
    mapping = c.evaluate_graph(graph, c)
    v = c.get_virtual_copy()
    new_entry = uuid4()
    v[new_entry] = DataFrame()

    # v[new_entry] does not hit c.cache
    assert len(c.cache) == 2
    assert len(v.cache) == 3 

    # the two cache_catalog are different
    assert new_entry not in c
    assert new_entry in v
    del v[new_entry]
    assert new_entry not in v
    for u in c:
        del v[u]
    assert len(v) == 0
    assert len(c) == 2


def test_eval_find_event_to_entity(process_creation_events):
    graph = process_creation_events
    stmt = "procs = FIND process RESPONDED es WHERE device.os = 'Linux' DISP procs"
    rets = parse_kestrel_and_update_irgraph(stmt, graph, {})
    graph = IRGraphEvaluable(graph)
    c = InMemoryCache()
    mapping = c.evaluate_graph(graph, c)
    assert len(rets) == 1
    df = mapping[rets[0].id]
    assert list(df.columns) == ['cmd_line', 'name', 'pid', 'uid', 'endpoint.uid', 'endpoint.name',
       'endpoint.os', 'file.name', 'file.path', 'user.uid', 'user.name',
       'user.type_id', 'parent_process.cmd_line', 'parent_process.name',
       'parent_process.pid', 'parent_process.uid',
       'parent_process.endpoint.uid', 'parent_process.endpoint.name',
       'parent_process.endpoint.os', 'user.endpoint.uid', 'user.endpoint.name',
       'user.endpoint.os', 'file.endpoint.uid', 'file.endpoint.name',
       'file.endpoint.os']
    assert df.shape[0] == 5  # WHERE clause filtered out 4 out of 9, so 5 remains


def test_eval_find_entity_to_event(process_creation_events, kestrel_config):
    graph = process_creation_events
    stmt = """
        procs = FIND process RESPONDED es WHERE device.os = 'Linux'
        eves = FIND event ORIGINATED BY procs
        DISP eves
    """
    rets = parse_kestrel_and_update_irgraph(stmt, graph, kestrel_config["entity_identifier"])
    graph = IRGraphEvaluable(graph)
    c = InMemoryCache()
    mapping = c.evaluate_graph(graph, c)
    assert len(rets) == 1
    df = mapping[rets[0].id]

    # 1. WHERE clause filtered out 4 out of 9, so 5 remains
    # 2. In the 5, 4 are not parent process of others, only the first is parent process
    # 3. There are 4 lines of logs/events that uses the parent process
    assert df.shape[0] == 4
    assert df.shape[1] == 48  # full event: the number of columns in the csv


def test_eval_find_entity_to_entity(process_creation_events, kestrel_config):
    graph = process_creation_events
    stmt = """
        procs = FIND process RESPONDED es WHERE device.os = 'Linux'
        parents = FIND process CREATED procs
        DISP parents
    """
    rets = parse_kestrel_and_update_irgraph(stmt, graph, kestrel_config["entity_identifier"])
    graph = IRGraphEvaluable(graph)
    c = InMemoryCache()
    mapping = c.evaluate_graph(graph, c)
    assert len(rets) == 1
    df = mapping[rets[0].id]

    # 1. WHERE clause filtered out 4 out of 9, so 5 remains
    # 2. The last 4 share the same parent
    # 3. So there are 2 processes returned/displayed after dedup
    assert df.shape[0] == 2
    assert list(df.columns) == ['cmd_line', 'name', 'pid', 'uid', 'endpoint.uid', 'endpoint.name', 'endpoint.os']
