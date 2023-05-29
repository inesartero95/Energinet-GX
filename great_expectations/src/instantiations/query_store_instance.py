from pathlib import Path
import sys
from typing import Protocol
from pyodbc import Connection

src = Path(__file__).parent.parent
sys.path.append(str(src))
from expectation_library.query_store import SynapseQueryStore

class MissingQueryStore(Exception):
    """Raised when the specified query store does not exist"""

class QueryStore(Protocol):
    dataset_name: str
    _conn: Connection

    def mean(self, column_name: str):
        ...

    def std(self, column_name: str):
        ...

class QueryStoreInstance:
    def __init__(self, type = "synapse") -> None:
        self.type = type

    def __get_synapse_store(self, connection_string: str, dataset_name: str):
        return SynapseQueryStore(connection_string=connection_string, dataset_name=dataset_name)
    
    def get_store(self, connection_string: str, dataset_name: str) -> QueryStore:
        if self.type == "synapse":
            return self.__get_synapse_store(connection_string, dataset_name)
        else:
            raise MissingQueryStore()