import logging
import os
from pathlib import Path
from typing import List, Mapping, Union

import pandas
import yaml
from kestrel.exceptions import (
    InvalidKestrelConfig,
    InvalidKestrelRelationTable,
    InvalidYamlInConfig,
)
from kestrel.utils import list_folder_files, load_data_file, update_nested_dict
from typeguard import typechecked

CONFIG_DIR_DEFAULT = Path.home() / ".config" / "kestrel"
CONFIG_PATH_DEFAULT = CONFIG_DIR_DEFAULT / "kestrel.yaml"
CONFIG_PATH_ENV_VAR = "KESTREL_CONFIG"  # override CONFIG_PATH_DEFAULT if provided

relations = []
entity_types = []

_logger = logging.getLogger(__name__)


@typechecked
def load_leaf_yaml(config: Mapping, path_dir: str) -> Mapping:
    new = {}
    for k, v in config.items():
        if isinstance(v, Mapping):
            new[k] = load_leaf_yaml(v, path_dir)
        elif isinstance(v, str) and v.endswith(".yaml"):
            try:
                if os.path.isabs(v):
                    with open(v, "r") as fp:
                        new[k] = yaml.safe_load(fp.read())
                else:
                    with open(os.path.join(path_dir, v), "r") as fp:
                        new[k] = yaml.safe_load(fp.read())
            except:
                raise InvalidYamlInConfig(v)
        else:
            new[k] = v
    return new


@typechecked
def load_default_config() -> Mapping:
    _logger.debug(f"Loading default config file...")
    default_config = load_data_file("kestrel.config", "kestrel.yaml")
    config_with_envvar_expanded = os.path.expandvars(default_config)
    config_content = yaml.safe_load(config_with_envvar_expanded)
    return config_content


@typechecked
def load_user_config(
    config_path_env_var: str, config_path_default: Union[str, Path]
) -> Mapping:
    config_path_default = config_path_default.absolute().as_posix()
    config_path = os.getenv(config_path_env_var, config_path_default)
    config_path = os.path.expanduser(config_path)
    config = {}
    if config_path:
        try:
            with open(config_path, "r") as fp:
                _logger.debug(f"User configuration file found: {config_path}")
                config = yaml.safe_load(os.path.expandvars(fp.read()))
            config = load_leaf_yaml(config, os.path.dirname(config_path))
        except FileNotFoundError:
            _logger.debug(f"User configuration file not exist.")
    return config


@typechecked
def load_kestrel_config() -> Mapping:
    config_default = load_default_config()
    config_user = load_user_config(CONFIG_PATH_ENV_VAR, CONFIG_PATH_DEFAULT)
    _logger.debug(f"User configuration loaded: {config_user}")
    _logger.debug(f"Updating default config with user config...")
    full_config = update_nested_dict(config_default, config_user)

    # valid the entity identifier section format
    for entity, idx in full_config["entity_identifier"].items():
        if not (isinstance(idx, list) and all((isinstance(x, str) for x in idx))):
            raise InvalidKestrelConfig(f"Invalid entity_identifier for '{entity}'")

    return full_config


@typechecked
def load_relation_configs(table_name: str) -> pandas.DataFrame:
    """Load relation tables

    Parameters:
        table_name: "entity" (entity-to-entity relation) or "event" (entity-to-event relation)

    Returns:
        Relation table in DataFrame, which has column names
    """
    filepaths = list(
        list_folder_files("kestrel.config", "relations", table_name, "csv")
    )
    if len(filepaths) > 1:
        _logger.error(f"More than one relation table found; will return the first one")
    try:
        table = pandas.read_csv(filepaths[0])
    except:
        raise InvalidKestrelRelationTable(filepaths[0])
    return table


@typechecked
def get_all_relations() -> List[str]:
    global relations
    if not relations:
        _relations = set()
        for filepath in list_folder_files(
            "kestrel.config", "relations", extension="csv"
        ):
            table = pandas.read_csv(filepath)
            _relations |= set(table["Relation"].to_list())
        relations = list(_relations)
    return relations
