import os

import yaml

from kestrel_interface_sqlalchemy.config import (
    PROFILE_PATH_ENV_VAR,
    Connection,
    load_config,
)

def test_load_config_w_default_map(tmp_path):
    config = {
        "connections": {
            "some-data-lake": {
                "url": "presto://jdoe@example.com:8889/hive",
            }
        },
        "datasources": {
            "cloud_table": {
                "connection": "some-data-lake",
                "table": "cloud_table",
                "timestamp": "eventTime",
                "timestamp_format": "%Y-%m-%d %H:%M:%S.%f",
            }
        }
    }
    config_file = tmp_path / "sqlalchemy.yaml"
    with open(config_file, 'w') as fp:
        yaml.dump(config, fp)
    os.environ[PROFILE_PATH_ENV_VAR] = str(config_file)
    read_config = load_config()
    assert read_config.datasources["cloud_table"].data_model_map["process"]["name"] == "process.name"
    assert read_config.datasources["cloud_table"].entity_identifier["process"] == "uid"

def test_load_config(tmp_path):
    config = {
        "connections": {
            "localhost": {
                "url": "sqlite:////home/jdoe/test.db",
            },
            "some-data-lake": {
                "url": "presto://jdoe@example.com:8889/hive",
            }
        },
        "datasources": {
            "cloud_table": {
                "connection": "some-data-lake",
                "table": "cloud_table",
                "timestamp": "eventTime",
                "timestamp_format": "%Y-%m-%d %H:%M:%S.%f",
                "data_model_map": str(tmp_path / "mapping.yaml"),
                "entity_identifier": "eid.yaml"
            }
        }
    }
    map_file = tmp_path / "mapping.yaml"
    with open(map_file, 'w') as fp:
        fp.write("some.field: other.field\n")
    eid_file = tmp_path / "eid.yaml"
    with open(eid_file, 'w') as fp:
        fp.write("process: pid\n")
    config_file = tmp_path / "sqlalchemy.yaml"
    with open(config_file, 'w') as fp:
        yaml.dump(config, fp)
    os.environ[PROFILE_PATH_ENV_VAR] = str(config_file)
    read_config = load_config()
    conn: Connection = read_config.connections["localhost"]
    assert conn.url == config["connections"]["localhost"]["url"]
    assert read_config.connections["localhost"].url == config["connections"]["localhost"]["url"]
    assert read_config.datasources["cloud_table"].timestamp == config["datasources"]["cloud_table"]["timestamp"]
    assert read_config.datasources["cloud_table"].table == config["datasources"]["cloud_table"]["table"]
    assert read_config.datasources["cloud_table"].data_model_map["some.field"] == "other.field"
    assert read_config.datasources["cloud_table"].entity_identifier["process"] == "pid"
