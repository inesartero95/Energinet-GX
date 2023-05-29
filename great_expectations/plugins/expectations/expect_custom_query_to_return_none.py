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


class ExpectCustomQueryToReturnNone(QueryExpectation):
    """Expect custom sql query to succeed by returning None.
    Args:
        template_dict:  custom_query
    """

    metric_dependencies = ("query.template_values",)

    query = """
            SELECT *
            FROM ({user_query}) 
            AS [MAIN]
    """

    success_keys = ("template_dict", "query")

    domain_keys = (
        "template_dict",
        "query",
        "batch_id",
        "row_condition" "condition_parser",
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
            "result": {"expectation": "expect_custom_query_to_succeed"},
        }

    examples = [
        {
            "data": [
                {
                    "dataset_name": "test",
                    "data": {
                        "col1": ["A", "A", "C", "C"],
                        "col2": ["1", "1", "3", "3"],
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
                            "user_query": "SELECT * FROM test WHERE col1='A'"
                        }
                    },
                    "out": {"success": False},
                },
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "user_query": "select * from test WHERE col2='6'"
                        }
                    },
                    "out": {"success": True},
                },
            ],
            "test_backends": [
                {
                    "backend": "sqlalchemy",
                    "dialects": ["sqlite"],
                }
            ],
        }
    ]

    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@InesPhilipsen"],
    }


if __name__ == "__main__":
    ExpectCustomQueryToReturnNone().print_diagnostic_checklist()
