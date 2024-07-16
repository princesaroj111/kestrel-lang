import os
import sqlite3
from collections import Counter

import pytest
import yaml
from kestrel_interface_sqlalchemy.config import PROFILE_PATH_ENV_VAR
from pandas import read_csv

from kestrel import Session
from kestrel.ir.filter import MultiComp
from kestrel.ir.instructions import DataSource, Filter, ProjectEntity, Variable


@pytest.fixture
def setup_sqlite_ecs_process_creation(tmp_path):
    """This setup datasource: sqlalchemy://events"""
    table_name = "events"
    test_dir = os.path.dirname(os.path.abspath(__file__))
    df = read_csv(os.path.join(test_dir, "logs_ecs_process_creation.csv"))
    sqlite_file = tmp_path / "fakelake.db"
    con = sqlite3.connect(sqlite_file)
    df.to_sql(name=table_name, con=con)
    interface_config = {
        "connections": {
            "datalake": {
                "url": "sqlite:///" + str(sqlite_file),
                "table_creation_permission": True,
            }
        },
        "datasources": {
            "events": {
                "connection": "datalake",
                "table": "events",
                "timestamp": "eventTime",
                "timestamp_format": "%Y-%m-%dT%H:%M:%S.%fZ",
            }
        }
    }
    config_file = tmp_path / "sqlalchemy.yaml"
    with open(config_file, mode="wt", encoding="utf-8") as f:
        yaml.dump(interface_config, f)
    os.environ[PROFILE_PATH_ENV_VAR] = str(config_file)
    yield None
    del os.environ[PROFILE_PATH_ENV_VAR]


@pytest.mark.parametrize(
    "where, ocsf_field", [
        ("name = 'bash'", "process.name"),
        ("command_line = 'bash'", "process.cmd_line"),  # ECS attribute
        ("entity_id = '1bf1d82d-aa83-4037-a748-3b2855fb29db'",  "process.uid"),# ECS attribute
        ("parent.name = 'abc'", "process.parent_process.name"),  # ECS attribute
        ("parent.pid = 1022", "process.parent_process.pid"),  # ECS attribute
    ]
)
def test_get_sinple_ecs_process(setup_sqlite_ecs_process_creation, where, ocsf_field):
    with Session() as session:
        stmt = f"procs = GET process FROM sqlalchemy://events WHERE {where}"
        session.execute(stmt)

        # first check the parsing is correct
        assert Counter(map(type, session.irgraph.nodes())) == Counter([DataSource, Variable, Filter, ProjectEntity])
        filt = session.irgraph.get_nodes_by_type(Filter)[0]
        # normalized to OCSF in IRGraph
        assert filt.exp.field == ocsf_field

        # now check for execution
        # - query translation to native
        # - result columns translation back to OCSF
        stmt = "DISP procs"
        df = session.execute(stmt)[0]
        assert len(df) == 1
        assert list(df.columns) == ['endpoint.uid', 'file.endpoint.uid', 'parent_process.endpoint.uid', 'parent_process.file.endpoint.uid', 'parent_process.user.endpoint.uid', 'user.endpoint.uid', 'endpoint.name', 'file.endpoint.name', 'parent_process.endpoint.name', 'parent_process.file.endpoint.name', 'parent_process.user.endpoint.name', 'user.endpoint.name', 'endpoint.os', 'file.endpoint.os', 'parent_process.endpoint.os', 'parent_process.file.endpoint.os', 'parent_process.user.endpoint.os', 'user.endpoint.os', 'cmd_line', 'name', 'pid', 'uid', 'file.name', 'file.path', 'file.parent_folder', 'parent_process.cmd_line', 'parent_process.name', 'parent_process.pid', 'parent_process.uid']

        # test value mapping: translate_dataframe()
        # OCSF to ECS file name: basename() as transformer specified in `ecs.yaml`
        # "/usr/bin/bash" -> "bash"
        # this also tests the passing of `from_obj_projection_base_field` along with CTE
        assert list(df["file.name"]) == ["bash"]


@pytest.mark.parametrize(
    "where, ocsf_fields", [
        ("process.name = 'bash'", ["process.name", "actor.process.name"]),
        ("process.parent.pid = 1022", ["process.parent_process.pid", "actor.process.parent_process.pid"]),
    ]
)
def test_get_sinple_event(setup_sqlite_ecs_process_creation, where, ocsf_fields):
    with Session() as session:
        stmt = f"evs = GET event FROM sqlalchemy://events WHERE {where}"
        session.execute(stmt)

        # first check the parsing is correct
        assert Counter(map(type, session.irgraph.nodes())) == Counter([DataSource, Variable, Filter, ProjectEntity])
        filt = session.irgraph.get_nodes_by_type(Filter)[0]
        # normalized to OCSF in IRGraph
        if isinstance(filt.exp, MultiComp):
            assert {x.field for x in filt.exp.comps} == set(ocsf_fields)
        else:
            assert filt.exp.field == ocsf_fields[0]

        # now check for execution
        # - query translation to native
        # - result columns translation back to OCSF
        stmt = "DISP evs"
        df = session.execute(stmt)[0]
        assert len(df) == 1
        assert len(list(df)) == 74

        # test value mapping: see previous test for more details
        assert list(df["process.file.name"]) == ["bash"]


def test_find_event_to_entity_no_filter(setup_sqlite_ecs_process_creation):
    with Session() as session:
        huntflow = """
        evs = GET event FROM sqlalchemy://events WHERE process.name = 'bash'
        procs = FIND process RESPONDED evs
        DISP procs
        """
        df = session.execute(huntflow)[0]

        # test result/column name translation
        assert list(df) == ['endpoint.uid', 'file.endpoint.uid', 'parent_process.endpoint.uid', 'parent_process.file.endpoint.uid', 'parent_process.user.endpoint.uid', 'user.endpoint.uid', 'endpoint.name', 'file.endpoint.name', 'parent_process.endpoint.name', 'parent_process.file.endpoint.name', 'parent_process.user.endpoint.name', 'user.endpoint.name', 'endpoint.os', 'file.endpoint.os', 'parent_process.endpoint.os', 'parent_process.file.endpoint.os', 'parent_process.user.endpoint.os', 'user.endpoint.os', 'cmd_line', 'name', 'pid', 'uid', 'file.name', 'file.path', 'file.parent_folder', 'parent_process.cmd_line', 'parent_process.name', 'parent_process.pid', 'parent_process.uid']

        # test result/value translation
        assert list(df["file.name"]) == ["bash"]
        assert list(df["file.path"]) == ["/usr/bin/bash"]


def test_find_event_to_entity(setup_sqlite_ecs_process_creation):
    with Session() as session:
        huntflow = """
        evs = GET event FROM sqlalchemy://events WHERE os.name IN ('Linux', 'Windows')
        DISP evs
        procs = FIND process RESPONDED evs WHERE endpoint.os = 'Linux'
        EXPLAIN procs
        DISP procs
        """
        evs, explain, procs = session.execute(huntflow)
        assert evs.shape[0] == 9  # all events

        stmt = explain.graphlets[0].query.statement
        test_dir = os.path.dirname(os.path.abspath(__file__))
        result_file = os.path.join(test_dir, "result_interface_find_event_to_entity.txt")
        with open(result_file) as h:
            result = h.read()
        assert stmt == result

        assert list(procs) == ['endpoint.uid', 'file.endpoint.uid', 'parent_process.endpoint.uid', 'parent_process.file.endpoint.uid', 'parent_process.user.endpoint.uid', 'user.endpoint.uid', 'endpoint.name', 'file.endpoint.name', 'parent_process.endpoint.name', 'parent_process.file.endpoint.name', 'parent_process.user.endpoint.name', 'user.endpoint.name', 'endpoint.os', 'file.endpoint.os', 'parent_process.endpoint.os', 'parent_process.file.endpoint.os', 'parent_process.user.endpoint.os', 'user.endpoint.os', 'cmd_line', 'name', 'pid', 'uid', 'file.name', 'file.path', 'file.parent_folder', 'parent_process.cmd_line', 'parent_process.name', 'parent_process.pid', 'parent_process.uid']
        assert procs.shape[0] == 5  # 5 Linux events -> 5 processes


def test_find_entity_to_event(setup_sqlite_ecs_process_creation):
    with Session() as session:
        huntflow = """
        evs = GET event FROM sqlalchemy://events WHERE os.name IN ('Linux', 'Windows')
        procs = FIND process RESPONDED evs WHERE endpoint.os = 'Linux'
        e2 = FIND event ORIGINATED BY procs
        EXPLAIN e2
        DISP e2
        """
        explain, e2 = session.execute(huntflow)

        stmt = explain.graphlets[0].query.statement
        test_dir = os.path.dirname(os.path.abspath(__file__))
        result_file = os.path.join(test_dir, "result_interface_find_entity_to_event.txt")
        with open(result_file) as h:
            result = h.read()
        assert stmt == result

        assert e2.shape[0] == 4
        assert list(e2["process.name"]) == ["uname", "cat", "ping", "curl"]
        assert e2.shape[1] == 74  # full event: refer to test_get_sinple_event() for number


def test_find_entity_to_entity(setup_sqlite_ecs_process_creation):
    with Session() as session:
        huntflow = """
        evs = GET event FROM sqlalchemy://events WHERE os.name IN ('Linux', 'Windows')
        procs = FIND process RESPONDED evs WHERE endpoint.os = 'Linux'
        parents = FIND process CREATED procs
        EXPLAIN parents
        DISP parents
        """
        explain, parents = session.execute(huntflow)

        stmt = explain.graphlets[0].query.statement
        test_dir = os.path.dirname(os.path.abspath(__file__))
        result_file = os.path.join(test_dir, "result_interface_find_entity_to_entity.txt")
        with open(result_file) as h:
            result = h.read()
        assert stmt == result

        assert parents.shape[0] == 2
        assert list(parents) == ['endpoint.uid', 'file.endpoint.uid', 'user.endpoint.uid', 'endpoint.name', 'file.endpoint.name', 'user.endpoint.name', 'endpoint.os', 'file.endpoint.os', 'user.endpoint.os', 'cmd_line', 'name', 'pid', 'uid']
