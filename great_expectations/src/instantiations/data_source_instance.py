from pathlib import Path
import sys
from typing import Protocol

src = Path(__file__).parent.parent
sys.path.append(str(src))
from services.data_source import SynapseDataSource

class MissingDataSource(Exception):
    """Raised when the specified data source does not exist"""

class DataSource(Protocol):
    datasource_name: str
    connection_str: str
    yaml_file: str

class DataSourceInstance:
    def __init__(self, type = "synapse") -> None:
        self.type = type

    def __get_synapse_datasource(self, datasource_name: str, connection_str: str):
        return SynapseDataSource(datasource_name = datasource_name, connection_string = connection_str)
    
    def get_datasource(self, datasource_name: str, connection_str: str) -> DataSource:
        if self.type == "synapse":
            return self.__get_synapse_datasource(datasource_name, connection_str)
        else:
            raise MissingDataSource()