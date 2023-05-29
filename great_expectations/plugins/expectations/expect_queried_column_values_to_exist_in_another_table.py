from os import environ
from sys import executable
from typing import Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)

environ["PYSPARK_PYTHON"] = executable
environ["PYSPARK_DRIVER_PYTHON"] = executable


class ExpectQueriedColumnValuesToExistInAnotherTable(QueryExpectation):
    """Expect all values in a specific column to exist 
    in another table's column.
    Args:
        template_dict: dict containing the following keys: \
             first_table_column (name of the main table column), \
             second_table_column (name of the column to compare to 
             in the second table), \
             second_table_full_name, \
    """

    metric_dependencies = ("query.template_values",)

    query = """
    SELECT * FROM {active_batch} a WHERE NOT EXISTS (
    SELECT 1 FROM {second_table_full_name} b
    WHERE b.{second_table_column} = a.{first_table_column}
    )
    """

    success_keys = ("template_dict", "query")

    domain_keys = (
        "query",
        "template_dict",
        "batch_id",
    )

    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
        "meta": None,
        "query": query,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)
    
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        metrics = convert_to_json_serializable(data=metrics)
        query_result = list(metrics.get("query.template_values"))

        success = not query_result
        return {
            "success": success,
            "result": {
                "Rows with IDs in first table missing in second table": query_result
            },
        }

    examples = [
        {
            "data": [
                {
                    "data": {
                        "adr": ["Odensevej 1", "Københavngade 1", "Odensevej 1"],
                    },
                },
                {   "dataset_name": "test_2",
                    "data": {
                        "adr_2": ["Odensevej 1", "Københavngade 1", "Århusvej 3"],
                        "other": ["Søren", "Peter", "2022-02-02"],
                    },
                },
            ],
            "tests": [
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "second_table_full_name": "test_2",
                            "first_table_column": "adr",
                            "second_table_column": "other",
                        },
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite", "spark"],
                },
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "second_table_full_name": "test_2",
                            "first_table_column": "adr",
                            "second_table_column": "adr_2",
                        }
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite", "spark"],
                },
            ],
            "test_backends": [
                {
                    "backend": "sqlalchemy",
                    "dialects": ["sqlite"],
                }
            ],
        },
    ]

    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@InesPhilipsen"],
    }

if __name__ == "__main__":
    ExpectQueriedColumnValuesToExistInAnotherTable().print_diagnostic_checklist()
