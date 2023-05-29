from pathlib import Path
import sys
from typing import Protocol

src = Path(__file__).parent.parent
sys.path.append(str(src))
from expectation_library.expectation_types import TableExpectations, ColumnExpectations
from instantiations.query_store_instance import QueryStore
from great_expectations.core.expectation_configuration import ExpectationConfiguration


class QueryStore(Protocol):
    
    def min(self, column: str) -> int:
        ...
    
    def mean(self, column: str) -> int:
        ...

    def std(self,  takcolumn: str) -> int:
        ...


class ExpectColumnValuesToNotBeOuliers_config(ColumnExpectations):
    """
    check for outliers; mean +- 2*std
    """

    type = 'data'

    def __init__(self, column_name: str, query_store: QueryStore):
        super().__init__(column_name)
        self.query_store = query_store
  
    def set_expectation_config(self) -> ExpectationConfiguration:
        mean = self.query_store.mean(self.column_name)
        std = self.query_store.std(self.column_name)
        lower_bound = mean - (4*std)
        upper_bound = mean + (4*std)
        kwargs = {"column": self.column_name,
                  "min_value" : lower_bound, 
                  "max_value" : upper_bound, 
                  #"mostly": 0.95
                  }
        # kwargs.update(self.info[self.column_name])
        expectation_configuration = ExpectationConfiguration(
            expectation_type = "expect_column_values_to_be_between",
            kwargs = kwargs,
        )
        return expectation_configuration
 