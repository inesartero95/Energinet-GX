
from typing import Any, Protocol, List

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from plugins.expectations.expect_queried_columnlist_to_have_no_duplicates import ExpectQueriedColumnlistToHaveNoDuplicates
from plugins.expectations.expect_queried_date_column_row_count_to_be import ExpectQueriedDateColumnRowCountToBe
from plugins.expectations.expect_queried_date_column_values_to_not_be_future_dates import ExpectQueriedDateColumnValuesToNotBeFutureDates
from plugins.expectations.expect_queried_date_column_to_have_no_days_missing import ExpectQueriedDateColumnToHaveNoDaysMissing
from plugins.expectations.expect_custom_query_to_return_none import ExpectCustomQueryToReturnNone

class ExpectQueriedDateColumnRowCountToBe_config:
    
    type = 'custom'

    def __init__(self, info:Any, asset_name: str) -> None:
        self.info = info
        self.asset_name = asset_name
    
    def set_expectation_config(self) -> ExpectationConfiguration:
        column_name = list(self.info.keys())[0]
        kwargs = {"column": column_name}
        kwargs.update(self.info[column_name])
        expectation_query = ExpectQueriedDateColumnRowCountToBe().query.format(active_batch = self.asset_name, col = column_name)
        kwargs.update({"query": expectation_query})
        expectation_configuration = ExpectationConfiguration(
                    expectation_type = "expect_queried_date_column_row_count_to_be",
                    kwargs = kwargs,
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": "This expectations check every unique date has n number of observations"
                        }
                    }
                )
        return expectation_configuration
    
class ExpectQueriedColumnlistToHaveNoDuplicates_config:

    type = 'custom'

    def __init__(self, info: str, asset_name: str) -> None:
        self.info = info #info.split(',')
        self.asset_name = asset_name

    def set_expectation_config(self) -> ExpectationConfiguration:
        # column_list = self.info #list(map(lambda s: s.strip(),self.info))
        column_dict = {"column_list": self.info}
        kwargs = {"template_dict" : column_dict}
        expectation_query = ExpectQueriedColumnlistToHaveNoDuplicates().query.format(active_batch = self.asset_name, column_list = self.info)
        kwargs.update({"query": expectation_query})
        expectation_configuration = ExpectationConfiguration(
                    expectation_type = "expect_queried_columnlist_to_have_no_duplicates",
                    kwargs = kwargs,
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": "This expectations should check ever row to be unique within the given columns"
                        }
                    }
                )
        return expectation_configuration

class ExpectQueriedDateColumnValuesToNotBeFutureDates_config:
    
    type = 'custom'

    def __init__(self, info: str, asset_name: str) -> None:
        self.info = info
        self.asset_name = asset_name

    def set_expectation_config(self) -> ExpectationConfiguration:
        column_name = self.info
        kwargs = {"column": column_name}
        expectation_query = ExpectQueriedDateColumnValuesToNotBeFutureDates().query.format(active_batch = self.asset_name, col = column_name)
        kwargs.update({"query": expectation_query})
        expectation_configuration = ExpectationConfiguration(
                    expectation_type = "expect_queried_date_column_values_to_not_be_future_dates",
                    kwargs = kwargs,
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": "This expectations check every that no date is in the future"
                        }
                    }
                )
        return expectation_configuration

class ExpectQueriedDateColumnToHaveNoDaysMissing_config:
    type = 'custom'

    def __init__(self, info: str, asset_name: str) -> None:
        self.info = info
        self.asset_name = asset_name

    def set_expectation_config(self) -> ExpectationConfiguration:
        column_name = list(self.info.keys())[0]
        kwargs = {"column": column_name}
        kwargs.update(self.info[column_name])
        expectation_query = ExpectQueriedDateColumnToHaveNoDaysMissing().query.format(active_batch = self.asset_name, col = column_name)
        kwargs.update({"query": expectation_query})
        expectation_configuration = ExpectationConfiguration(
                            expectation_type = "expect_queried_date_column_to_have_no_days_missing",
                            kwargs = kwargs,
                            meta={
                                "notes": {
                                    "format": "markdown",
                                    "content": "This expectations check every unique date has n number of observations"
                                }
                            }
                        )
        return expectation_configuration

class ExpectCustomQueryToReturnNone_config:
    type = 'custom'

    def __init__(self, info: str, asset_name : str) -> None:
        self.info = info
    
    def set_expectation_config(self) -> ExpectationConfiguration:
        kwargs = {"template_dict" : self.info}
        expectation_configuration = ExpectationConfiguration(
                            expectation_type = "expect_custom_query_to_return_none",
                            kwargs = kwargs,
                            meta={
                                "notes": {
                                    "format": "markdown",
                                    "content": "This expectations check custom query returns no rows"
                                }
                            }
                        )
        return expectation_configuration
