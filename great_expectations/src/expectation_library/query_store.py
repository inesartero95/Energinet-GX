from typing import Protocol
import sys
from pathlib import Path
import pyodbc
import pandas as pd

src = Path(__file__).parent.parent.parent
sys.path.append(str(src))

from utility.ODBC_connect import ODBC

class SynapseQueryStore:
    
    def __init__(self, connection_string: str, dataset_name: str) -> None:
        self.dataset_name = dataset_name
        self._conn = pyodbc.connect(connection_string)
        # self.cursor = self._conn.cursor()
      
    def mean(self, column_name: str):
        query = f"SELECT AVG({column_name}) FROM {self.dataset_name}"
        mean = pd.read_sql(query, self._conn)
        return mean.iat[0, 0]

    def std(self, column_name: str):
        query = f"SELECT STDEV({column_name}) FROM {self.dataset_name}"
        std = pd.read_sql(query, self._conn)
        return std.iat[0, 0]

   