
import pandas as pd
import os
from typing import Any, Protocol

from great_expectations.core.expectation_configuration import ExpectationConfiguration

import pandas as pd 

# generate_completness_suite.validator.expect_column_values_to_not_be_null(column = 'MunicipalityCode')


class ExpectColumnValuesToNotBeNull_config:
    """
    completness should check for no null values and the number of rows match the expected number.
    """

    type = 'static'

    def __init__(self, info: Any) -> None:
        self.info = info
    
    def set_expectation_config(self) -> ExpectationConfiguration:
        column_name = self.info 

        expectation_configuration = ExpectationConfiguration(
            expectation_type = "expect_column_values_to_not_be_null",
            kwargs={
                "column": column_name,
                "mostly": 1.0,
            },
            meta={
                "notes": {
                    "format": "markdown",
                    "content": "Some clever comment about this expectation. **Markdown** `Supported`"
                }
            }
        )
        return expectation_configuration


class ExpectTableColumnsToMatchSet_config:
    
    type = 'static'


    def __init__(self, info: str) -> None:
        self.info = info.split(',')

    def set_expectation_config(self) -> ExpectationConfiguration:
        expectation_configuration = ExpectationConfiguration(
            # Name of expectation type being added
            expectation_type="expect_table_columns_to_match_set",
            # These are the arguments of the expectation
            kwargs={
                "column_set": list(map(lambda s: s.strip(),self.info)),
                "exact_match" : False
            },
            # This is how you can optionally add a comment about this expectation.
            meta={
                "notes": {
                    "format": "markdown",
                    "content": "Some clever comment about this expectation. **Markdown** `Supported`"
                }
            }
        )
        return expectation_configuration
    

class ExpectColumnValuesToBeBetween_config:
    """
    check extreme values
    """

    type = 'static'

    def __init__(self, info: Any) -> None:
        self.info = info
  
    def set_expectation_config(self) -> ExpectationConfiguration:
        column_name = list(self.info.keys())[0]
        kwargs = {"column": column_name}
        kwargs.update(self.info[column_name])
        expectation_configuration = ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs=kwargs,
        )
        return expectation_configuration

class ExpectCompoundColumnValuesToBeUnique:
    """
    check for dublicates across columns
    """

    type = 'static'

    def __init__(self, info: str) -> None:
        self.info = info.split(',')

    def set_expectation_config(self) -> ExpectationConfiguration:
        expectation_configuration = ExpectationConfiguration(
            # Name of expectation type being added
            expectation_type="expect_compound_columns_to_be_unique",
            # These are the arguments of the expectation
            kwargs={
                "column_list": list(map(lambda s: s.strip(),self.info))
            },
            # This is how you can optionally add a comment about this expectation.
            meta={
                "notes": {
                    "format": "markdown",
                    "content": "Some clever comment about this expectation. **Markdown** `Supported`"
                }
            }
        )
        return expectation_configuration
    

class ExpectColumnValuesToBeUnique:
    """
    check for dublicates in a single column
    """

    type = 'static'

    def __init__(self, info: Any) -> None:
        self.info = info
    
    def set_expectation_config(self) -> ExpectationConfiguration:
        column_name = self.info 

        expectation_configuration = ExpectationConfiguration(
            expectation_type = "expect_column_values_to_be_unique",
            kwargs={
                "column": column_name,
                "mostly": 1.0,
            },
            meta={
                "notes": {
                    "format": "markdown",
                    "content": "Some clever comment about this expectation. **Markdown** `Supported`"
                }
            }
        )
        return expectation_configuration