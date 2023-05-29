import pyodbc
# from pyodbc import Row
import configparser
import os
from typing import Any
from urllib import parse

class ODBC:
    def __init__(self, server: Any, authentication) -> None:
        self.server = server
        self.authentication = authentication
        self._conn_str = f"Driver={self.server['driver']};Server=tcp:{self.server['name']},1433; Database={self.server['database']};Uid={self.server['username']};Pwd={self.server['password']};TrustServerCertificate=no;Authentication={self.authentication}"

    @property
    def url_conn_str(self):
        "Return connection string"
        params = parse.quote_plus(self._conn_str)
        url_conn_str = 'mssql+pyodbc:///?odbc_connect=%s' % params
        return url_conn_str
    

if __name__ == '__main__':
    script_path = os.path.realpath(os.path.dirname(__file__))

    os.chdir(script_path)

    config = configparser.ConfigParser()

    name = "wind"
    schema_name = "datahub_bi"

    config.read('..\secrets.ini')
    server = config['Server info']

    odbc = ODBC(server, "ActiveDirectoryPassword")
   