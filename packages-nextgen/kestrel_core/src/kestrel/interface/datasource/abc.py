import json
from abc import ABC, abstractmethod
from pandas import DataFrame
from uuid import UUID
from typing import (
    Mapping,
    Union,
)

from kestrel.ir.instructions import Reference
from kestrel.ir.graph import IRGraphSoleInterface
from kestrel.exceptions import (
    UnresolvedReference,
    InvalidSerializedDatasourceInterfaceCacheCatalog,
)


class AbstractDataSourceInterface(ABC):
    """Abstract class for datasource interface

    Concepts:

    - Think an interface as a datalake

    - Think a datasource as a table in the datalake

    Attributes:

        datasources: map a datasource name to datalake table name

        cache_catalog: map a cached item (instruction.id) to datalake table/view name
    """

    def __init__(self, serialized_cache_catalog: Union[None, str] = None):
        self.datasources: Mapping[str, str] = {}
        self.cache_catalog: Mapping[UUID, str] = {}

        if serialized_cache_catalog:
            try:
                self.cache_catalog = json.loads(serialized_cache_catalog)
            except:
                raise InvalidSerializedDatasourceInterfaceCacheCatalog()

    def __contains__(self, instruction_id: UUID) -> bool:
        """Whether a datasource is in the interface

        Parameters:

            instruction_id: id of the instruction
        """
        return instruction_id in self.cache_catalog

    @abstractmethod
    def store(
        self,
        instruction_id: UUID,
        data: DataFrame,
        session_id: Union[None, UUID] = None,
    ):
        """Create a new table in the datalake from a dataframe

        The name of the table is a function of instruction_id (and session_id)
        in case there are conflicting tables in the datalake.

        The function can be implemented as a hashtable. If the hash collides
        with an existing hash, figure out whether the existing hash/table is
        used by the current interface and session. If yes, then replace; if
        not, then generate a new random value and record in self.cache_catalog.

        This method will update self.cache_catalog.

        Parameters:

            instruction_id: the key to be placed in `self.cache_catalog`

            data: the dataframe to store

            session_id: the optional information to derive table name in datalake
        """
        ...

    @abstractmethod
    def evaluate_graph(
        self, g: IRGraphSoleInterface, all_variables_in_return: bool = False
    ) -> Mapping[UUID, DataFrame]:
        """Evaluate the IRGraph

        Parameters:

            g: The IRGraph with zero or one interface

            all_variables_in_return: include evaluation results on all variables in return

        Returns:

            By default, return the dataframes for each sink node in the graph.
            If all_variables_in_return == True, also include dataframes for
            each variable node in the return.
        """
        # requirement: g should not have any Reference node
        refs = self.get_nodes_by_type(Reference)
        if refs:
            raise UnresolvedReference(refs)

    def cache_catalog_to_json(self) -> str:
        """Serialize the cache catalog to a JSON string"""
        return json.dumps(self.cache_catalog)