
from typing import Protocol, List

class TableExpectations:
    def __init__(self, column_list: List[str]):
        self.column_list = column_list

class ColumnExpectations:
    def __init__(self, column_name: str):
        self.column_name = column_name