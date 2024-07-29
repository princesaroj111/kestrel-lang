import json
import os
from uuid import uuid4

import pytest
from pandas import DataFrame, read_csv

from kestrel import Session
from kestrel.cache import SqlCache
from kestrel.config.internal import CACHE_INTERFACE_IDENTIFIER
from kestrel.display import GraphExplanation
from kestrel.frontend.parser import parse_kestrel_and_update_irgraph
from kestrel.ir.graph import IRGraph, IRGraphEvaluable
from kestrel.ir.instructions import Construct, SerializableDataFrame
from kestrel.exceptions import EntityNotFound


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


def test_execute_in_cache():
    hf = """
proclist = NEW process [ {"name": "cmd.exe", "pid": 123}
                       , {"name": "explorer.exe", "pid": 99}
                       , {"name": "firefox.exe", "pid": 201}
                       , {"name": "chrome.exe", "pid": 205}
                       ]
browsers = proclist WHERE name != "cmd.exe"
DISP browsers
cmd = proclist WHERE name = "cmd.exe"
DISP cmd ATTR pid
"""
    b1 = DataFrame([ {"name": "explorer.exe", "pid": 99}
                   , {"name": "firefox.exe", "pid": 201}
                   , {"name": "chrome.exe", "pid": 205}
                   ])
    b2 = DataFrame([ {"pid": 123} ])
    with Session() as session:
        res = session.execute_to_generate(hf)
        assert b1.equals(next(res))
        assert b2.equals(next(res))
        with pytest.raises(StopIteration):
            next(res)


def test_execute_in_cache_stix_process():
    hf = """
proclist = NEW process [ {"file.name": "cmd.exe", "pid": 123}
                       , {"file.name": "explorer.exe", "pid": 99}
                       , {"file.name": "firefox.exe", "pid": 201}
                       , {"file.name": "chrome.exe", "pid": 205}
                       ]
DISP proclist ATTR binary_ref.name
"""
    b1 = DataFrame([ {"file.name": "cmd.exe"}
                   , {"file.name": "explorer.exe"}
                   , {"file.name": "firefox.exe"}
                   , {"file.name": "chrome.exe"}
                   ])
    with Session() as session:
        res = session.execute_to_generate(hf)
        assert b1.equals(next(res))
        with pytest.raises(StopIteration):
            next(res)


@pytest.mark.skip("TODO: need attr mapping for Construct")
def test_execute_in_cache_stix_process_ocsf_disp_attr():
    hf = """
proclist = NEW process [ {"binary_ref.name": "cmd.exe", "pid": 123}
                       , {"binary_ref.name": "explorer.exe", "pid": 99}
                       , {"binary_ref.name": "firefox.exe", "pid": 201}
                       , {"binary_ref.name": "chrome.exe", "pid": 205}
                       ]
DISP proclist ATTR file.name
"""
    b1 = DataFrame([ {"file.name": "cmd.exe"}
                   , {"file.name": "explorer.exe"}
                   , {"file.name": "firefox.exe"}
                   , {"file.name": "chrome.exe"}
                   ])
    with Session() as session:
        res = session.execute_to_generate(hf)
        assert b1.equals(next(res))
        with pytest.raises(StopIteration):
            next(res)


def test_execute_in_cache_stix_process_filtered():
    hf = """
proclist = NEW process [ {"file.name": "cmd.exe", "pid": 123}
                       , {"file.name": "explorer.exe", "pid": 99}
                       , {"file.name": "firefox.exe", "pid": 201}
                       , {"file.name": "chrome.exe", "pid": 205}
                       ]
browsers = proclist WHERE binary_ref.name in ('chrome.exe', 'firefox.exe')
DISP browsers ATTR binary_ref.name, pid
"""
    b1 = DataFrame([ {"file.name": "firefox.exe", "pid": 201}
                   , {"file.name": "chrome.exe", "pid": 205}
                   ])
    with Session() as session:
        res = session.execute_to_generate(hf)
        df = next(res)
        assert b1.equals(df)
        with pytest.raises(StopIteration):
            next(res)


def test_execute_in_cache_stix_process_with_ref_and_multi_returns():
    hf = """
proclist = NEW process [ {"file.name": "cmd.exe", "pid": 123}
                       , {"file.name": "explorer.exe", "pid": 99}
                       , {"file.name": "firefox.exe", "pid": 201}
                       , {"file.name": "chrome.exe", "pid": 205}
                       ]
newvar = proclist WHERE binary_ref.name = "cmd.exe"
DISP proclist ATTR binary_ref.name
newvar2 = proclist WHERE binary_ref.name IN ("explorer.exe", "cmd.exe")
newvar3 = newvar2 WHERE pid IN newvar.pid
DISP newvar3 ATTR binary_ref.name
"""
    b1 = DataFrame([ {"file.name": "cmd.exe"}
                   , {"file.name": "explorer.exe"}
                   , {"file.name": "firefox.exe"}
                   , {"file.name": "chrome.exe"}
                   ])
    b2 = DataFrame([ {"file.name": "cmd.exe"}
                   ])
    with Session() as session:
        res = session.execute_to_generate(hf)
        assert b1.equals(next(res))
        assert b2.equals(next(res))
        with pytest.raises(StopIteration):
            next(res)


def test_execute_in_cache_stix_file():
    data = [ {"name": "cmd.exe", "hashes.MD5": "AD7B9C14083B52BC532FBA5948342B98"}
           , {"name": "powershell.exe", "hashes.MD5": "04029E121A0CFA5991749937DD22A1D9"}
    ]
    hf = f"""
filelist = NEW file {json.dumps(data)}
DISP filelist ATTR name, hashes.MD5
"""
    b1 = DataFrame(data)
    with Session() as session:
        res = session.execute_to_generate(hf)
        assert b1.equals(next(res))
        with pytest.raises(StopIteration):
            next(res)


def test_double_deref_in_cache():
    # When the Filter node is dereferred twice
    # The node should be deepcopied each time to avoid issue
    hf = """
proclist = NEW process [ {"name": "cmd.exe", "pid": 123}
                       , {"name": "explorer.exe", "pid": 99}
                       , {"name": "firefox.exe", "pid": 201}
                       , {"name": "chrome.exe", "pid": 205}
                       ]
px = proclist WHERE name != "cmd.exe" AND pid = 205
chrome = proclist WHERE pid IN px.pid
DISP chrome
DISP chrome
"""
    df = DataFrame([ {"name": "chrome.exe", "pid": 205} ])
    with Session() as session:
        res = session.execute_to_generate(hf)
        assert df.equals(next(res))
        assert df.equals(next(res))
        with pytest.raises(StopIteration):
            next(res)


def test_explain_in_cache():
    hf = """
proclist = NEW process [ {"name": "cmd.exe", "pid": 123}
                       , {"name": "explorer.exe", "pid": 99}
                       , {"name": "firefox.exe", "pid": 201}
                       , {"name": "chrome.exe", "pid": 205}
                       ]
browsers = proclist WHERE name != "cmd.exe"
chrome = browsers WHERE pid = 205
EXPLAIN chrome
"""
    with Session() as session:
        ress = session.execute_to_generate(hf)
        res = next(ress)
        assert isinstance(res, GraphExplanation)
        assert len(res.graphlets) == 1
        ge = res.graphlets[0]
        assert ge.graph == IRGraphEvaluable(session.irgraph).to_dict()
        construct = session.irgraph.get_nodes_by_type(Construct)[0]
        assert ge.action.language == "SQL"
        stmt = ge.action.statement.replace('"', '')
        assert stmt == f"WITH proclist AS \n(SELECT DISTINCT * \nFROM {construct.id.hex}v), \nbrowsers AS \n(SELECT DISTINCT * \nFROM proclist \nWHERE name != 'cmd.exe'), \nchrome AS \n(SELECT DISTINCT * \nFROM browsers \nWHERE pid = 205)\n SELECT DISTINCT * \nFROM chrome"
        with pytest.raises(StopIteration):
            next(ress)


def test_multi_interface_explain():

    class DataLake(SqlCache):
        @staticmethod
        def schemes():
            return ["datalake"]

    class Gateway(SqlCache):
        @staticmethod
        def schemes():
            return ["gateway"]

    with Session() as session:
        stmt1 = """
procs = NEW process [ {"name": "cmd.exe", "pid": 123}
                    , {"name": "explorer.exe", "pid": 99}
                    , {"name": "firefox.exe", "pid": 201}
                    , {"name": "chrome.exe", "pid": 205}
                    ]
DISP procs
"""
        session.execute(stmt1)
        session.interface_manager[CACHE_INTERFACE_IDENTIFIER].__class__ = DataLake
        session.irgraph.get_nodes_by_type_and_attributes(Construct, {"interface": CACHE_INTERFACE_IDENTIFIER})[0].interface = "datalake"

        new_cache = SqlCache()
        session.interface_manager.interfaces.append(new_cache)
        stmt2 = """
nt = NEW event [ {"abc": 123, "source": "192.168.1.1", "destination": "1.1.1.1"}
               , {"abc": 205, "source": "192.168.1.1", "destination": "1.1.1.2"}
               ]
DISP nt
"""
        session.execute(stmt2)
        session.interface_manager[CACHE_INTERFACE_IDENTIFIER].__class__ = Gateway
        session.irgraph.get_nodes_by_type_and_attributes(Construct, {"interface": CACHE_INTERFACE_IDENTIFIER})[0].interface = "gateway"

        new_cache = SqlCache()
        session.interface_manager.interfaces.append(new_cache)
        stmt3 = """
domain = NEW domain [ {"ip": "1.1.1.1", "domain": "cloudflare.com"}
                    , {"ip": "1.1.1.2", "domain": "xyz.cloudflare.com"}
                    ]
DISP domain
"""
        session.execute(stmt3)

        stmt = """
p2 = procs WHERE name IN ("firefox.exe", "chrome.exe")
ntx = nt WHERE abc IN p2.pid
d2 = domain WHERE ip IN ntx.destination
EXPLAIN d2
DISP d2
"""
        ress = session.execute_to_generate(stmt)
        disp = next(ress)
        df_res = next(ress)

        with pytest.raises(StopIteration):
            next(ress)

        assert isinstance(disp, GraphExplanation)
        assert len(disp.graphlets) == 4

        # DISP procs
        assert len(disp.graphlets[0].graph["nodes"]) == 5
        query = disp.graphlets[0].action.statement.replace('"', '')
        procs = session.irgraph.get_variable("procs")
        c1 = next(session.irgraph.predecessors(procs))
        assert query == f"WITH procs AS \n(SELECT DISTINCT * \nFROM {c1.id.hex}), \np2 AS \n(SELECT DISTINCT * \nFROM procs \nWHERE name IN ('firefox.exe', 'chrome.exe'))\n SELECT DISTINCT pid \nFROM p2"

        # DISP nt
        assert len(disp.graphlets[1].graph["nodes"]) == 2
        query = disp.graphlets[1].action.statement.replace('"', '')
        nt = session.irgraph.get_variable("nt")
        c2 = next(session.irgraph.predecessors(nt))
        assert query == f"WITH nt AS \n(SELECT DISTINCT * \nFROM {c2.id.hex})\n SELECT DISTINCT * \nFROM nt"

        # DISP domain
        assert len(disp.graphlets[2].graph["nodes"]) == 2
        query = disp.graphlets[2].action.statement.replace('"', '')
        domain = session.irgraph.get_variable("domain")
        c3 = next(session.irgraph.predecessors(domain))
        assert query == f"WITH domain AS \n(SELECT DISTINCT * \nFROM {c3.id.hex})\n SELECT DISTINCT * \nFROM domain"

        # EXPLAIN d2
        assert len(disp.graphlets[3].graph["nodes"]) == 11
        query = disp.graphlets[3].action.statement.replace('"', '')
        p2 = session.irgraph.get_variable("p2")
        p2pa = next(session.irgraph.successors(p2))
        assert query == f"WITH ntx AS \n(SELECT DISTINCT * \nFROM {nt.id.hex}v \nWHERE abc IN (SELECT DISTINCT * \nFROM {p2pa.id.hex}v)), \nd2 AS \n(SELECT DISTINCT * \nFROM {domain.id.hex}v \nWHERE ip IN (SELECT DISTINCT destination \nFROM ntx))\n SELECT DISTINCT * \nFROM d2"

        df_ref = DataFrame([{"ip": "1.1.1.2", "domain": "xyz.cloudflare.com"}])
        assert df_ref.equals(df_res)


def test_apply_on_construct():
    hf = """
proclist = NEW process [ {"name": "cmd.exe", "pid": 123}
                       , {"name": "explorer.exe", "pid": 99}
                       , {"name": "firefox.exe", "pid": 201}
                       , {"name": "chrome.exe", "pid": 205}
                       ]
APPLY python://something ON proclist WITH foo=abc,bar=1,baz=1.5
DISP proclist ATTR name, foo, bar, baz
"""
    b1 = DataFrame([ {"name": "cmd.exe", "foo": "abc", "bar": 1, "baz": 1.5}
                   , {"name": "explorer.exe", "foo": "abc", "bar": 1, "baz": 1.5}
                   , {"name": "firefox.exe", "foo": "abc", "bar": 1, "baz": 1.5}
                   , {"name": "chrome.exe", "foo": "abc", "bar": 1, "baz": 1.5}
                   ])
    with Session() as session:
        # Add test analytic
        test_dir = os.path.dirname(os.path.abspath(__file__))
        session.interface_manager["python"].config["something"] = {
            "module": os.path.join(test_dir, "test_analytic.py"),
            "func": "do_something"
        }
        res = session.execute_to_generate(hf)
        disp = next(res)
        assert b1.equals(disp)
        with pytest.raises(StopIteration):
            next(res)


def test_apply_on_construct_use_env():
    hf = """
proclist = NEW process [ {"name": "cmd.exe", "pid": 123}
                       , {"name": "explorer.exe", "pid": 99}
                       , {"name": "firefox.exe", "pid": 201}
                       , {"name": "chrome.exe", "pid": 205}
                       ]
APPLY python://something ON proclist WITH name=foo,value=1
DISP proclist ATTR name, foo
"""
    b1 = DataFrame([ {"name": "cmd.exe", "foo": 1}
                   , {"name": "explorer.exe", "foo": 1}
                   , {"name": "firefox.exe", "foo": 1}
                   , {"name": "chrome.exe", "foo": 1}
                   ])
    with Session() as session:
        # Add test analytic
        test_dir = os.path.dirname(os.path.abspath(__file__))
        session.interface_manager["python"].config["something"] = {
            "module": os.path.join(test_dir, "test_analytic.py"),
            "func": "do_something_env"
        }
        res = session.execute_to_generate(hf)
        disp = next(res)
        assert b1.equals(disp)
        with pytest.raises(StopIteration):
            next(res)


def test_explain_find_event_to_entity(process_creation_events):
    with Session() as session:
        session.irgraph = process_creation_events
        res = session.execute("procs = FIND process RESPONDED es WHERE device.os = 'Linux' EXPLAIN procs")[0]
        construct = session.irgraph.get_nodes_by_type(Construct)[0]
        stmt = res.graphlets[0].action.statement.replace('"', '')
        # cache.sql will use "*" as columns for __setitem__ in virtual cache
        # so the result is different from test_cache_sqlite::test_explain_find_event_to_entity
        assert stmt == f"WITH es AS \n(SELECT DISTINCT * \nFROM {construct.id.hex}v), \nprocs AS \n(SELECT DISTINCT * \nFROM es \nWHERE device.os = \'Linux\')\n SELECT DISTINCT * \nFROM procs"


def test_get_nonexist_entity(process_creation_events):
    with Session() as session:
        session.irgraph = process_creation_events
        with pytest.raises(EntityNotFound):
            session.execute("reg = FIND reg_key RESPONDED es WHERE device.os = 'Linux' DISP reg")
