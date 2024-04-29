import os

import yaml

from kestrel_interface_sqlalchemy.config import (
    PROFILE_PATH_ENV_VAR,
    Connection,
    load_config,
)


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
                "data_model_mapping": str(tmp_path / "mapping.yaml")
            }
        }
    }
    map_file = tmp_path / "mapping.yaml"
    with open(map_file, 'w') as fp:
        fp.write("some.field: other.field\n")
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