from typing import Protocol, Any
from great_expectations.core.expectation_configuration import ExpectationConfiguration
import sys
from pathlib import Path

src = Path(__file__).parent.parent
sys.path.append(str(src))
from expectation_library.data_expectations import ExpectColumnValuesToNotBeOuliers_config, QueryStore
from expectation_library.static_expectations import (
    ExpectColumnValuesToNotBeNull_config, 
    ExpectTableColumnsToMatchSet_config, 
    ExpectColumnValuesToBeBetween_config,
    )
from expectation_library.custom_expectations import (
    ExpectQueriedDateColumnRowCountToBe_config,
    ExpectQueriedColumnlistToHaveNoDuplicates_config,
    ExpectQueriedDateColumnValuesToNotBeFutureDates_config,
    ExpectQueriedDateColumnToHaveNoDaysMissing_config,
    ExpectCustomQueryToReturnNone_config,
    )

class GenericExpectation(Protocol):
    """_summary_

    Parameters
    ----------
    Protocol : _type_
        Can be either static, data or custom
    """
    type: str


def factory(expectation_name: str) -> type[GenericExpectation]:
    expectations = {
        "expected_columns_not_null": ExpectColumnValuesToNotBeNull_config,
        "expected_columns_exists": ExpectTableColumnsToMatchSet_config,
        "expected_columns_with_extreme_values": ExpectColumnValuesToBeBetween_config,
        "expected_columns_check_outliers": ExpectColumnValuesToNotBeOuliers_config,
        "expected_rows_per_date" : ExpectQueriedDateColumnRowCountToBe_config,
        "expected_columns_to_have_no_duplicates" : ExpectQueriedColumnlistToHaveNoDuplicates_config,
        "expected_dates_not_to_be_in_future" : ExpectQueriedDateColumnValuesToNotBeFutureDates_config,
        "expected_dates_not_to_have_no_missing_days": ExpectQueriedDateColumnToHaveNoDaysMissing_config,
        "expected_query_to_pass" : ExpectCustomQueryToReturnNone_config,
    }
    return expectations[expectation_name]

