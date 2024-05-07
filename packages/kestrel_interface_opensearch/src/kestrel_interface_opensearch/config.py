import logging
from dataclasses import dataclass
from typing import Dict, Optional

from mashumaro.mixins.json import DataClassJSONMixin

from kestrel.config.utils import (
    CONFIG_DIR_DEFAULT,
    load_user_config,
    load_kestrel_config,
)
from kestrel.exceptions import InterfaceNotConfigured
from kestrel.mapping.data_model import load_default_mapping


PROFILE_PATH_DEFAULT = CONFIG_DIR_DEFAULT / "opensearch.yaml"
PROFILE_PATH_ENV_VAR = "KESTREL_OPENSEARCH_CONFIG"

_logger = logging.getLogger(__name__)


@dataclass
class Auth:
    username: str
    password: str


@dataclass
class Connection(DataClassJSONMixin):
    url: str
    auth: Auth
    verify_certs: bool = True

    def __post_init__(self):
        self.auth = Auth(**self.auth)


@dataclass
class DataSource(DataClassJSONMixin):
    connection: str
    index_pattern: str
    timestamp: str
    timestamp_format: str
    data_model_map: Optional[Dict] = None
    entity_identifier: Optional[Dict] = None

    def __post_init__(self):
        if not self.data_model_map:
            # Default to the built-in ECS mapping
            self.data_model_map = load_default_mapping("ecs")


@dataclass
class Config(DataClassJSONMixin):
    connections: Dict[str, Connection]
    datasources: Dict[str, DataSource]

    def __post_init__(self):
        self.connections = {k: Connection(**v) for k, v in self.connections.items()}
        self.datasources = {k: DataSource(**v) for k, v in self.datasources.items()}


def load_config():
    try:
        interface_config = Config(
            **load_user_config(PROFILE_PATH_ENV_VAR, PROFILE_PATH_DEFAULT)
        )

        # load default entity identifier from main Kestrel config
        kestrel_config = load_kestrel_config()
        for ds in interface_config.datasources.values():
            if not ds.entity_identifier:
                ds.entity_identifier = kestrel_config["entity_identifier"]

        return interface_config
    except TypeError:
        raise InterfaceNotConfigured()
