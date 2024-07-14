import pytest
import yaml
import os
import sqlite3
from collections import Counter
from pandas import read_csv

from kestrel import Session
from kestrel.ir.filter import MultiComp
from kestrel.ir.instructions import DataSource, Variable, Filter, ProjectEntity
from kestrel_interface_sqlalchemy.config import PROFILE_PATH_ENV_VAR


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
