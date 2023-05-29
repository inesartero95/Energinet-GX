"""
This is an example of a Custom QueryExpectation.
For detailed information on QueryExpectations, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""
from os import environ
from sys import executable
from typing import Optional, Union, Any, List

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)

from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import (
    RenderedStringTemplateContent,
    RenderedTableContent,
    RenderedBulletListContent,
    RenderedGraphContent,
)
from great_expectations.render.util import substitute_none_for_missing

environ["PYSPARK_PYTHON"] = executable
environ["PYSPARK_DRIVER_PYTHON"] = executable


class ExpectQueriedColumnlistToHaveNoDuplicates(QueryExpectation):
    """Expect the number of observation of
    the given columns to be unique.

    Args:
        "template_dict":
            column_list (columns to check uniqueness on separated by comma)
    """

    metric_dependencies = ("query.template_values",)

    query = """
            SELECT {column_list}, COUNT(*) AS [_count]
            FROM {active_batch}
            GROUP BY {column_list}
            """

    success_keys = ("template_dict", "query")

    domain_keys = (
        "query",
        "template_dict",
        "batch_id",
        "row_condition",
        "condition_parser",
    )

    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
        "meta": None,
        "query": query,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
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
        configuration["kwargs"].get("columns")

        query_result = list(metrics.get("query.template_values")) 

        duplicates = [list(d.values()) for d in query_result if d["_count"] > 1]
        
        n = sum(d["_count"] for d in query_result)

        if len(query_result) == n:
            unexpected_percentage = 0
        else:
            unexpected_percentage = 100 - len(query_result)/n * 100

        success = all(d["_count"] == 1 for d in query_result)

        return {
            "success": success,
            "result": {
                "observed_value": f"\u2248 {round(unexpected_percentage, 5)} % unexpected. \n unexpected_list: {duplicates}",
            },
        }

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: ExpectationConfiguration = None,
        result: ExpectationValidationResult = None,
        language: str = None,
        runtime_configuration: dict = None,
        **kwargs,
    ) -> List[
        Union[
            dict,
            str,
            RenderedStringTemplateContent,
            RenderedTableContent,
            RenderedBulletListContent,
            RenderedGraphContent,
            Any,
        ]
    ]:
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "columns",
                "query",
                "condition_parser",
                "row_condition_parser",
            ],
        )

        # build string template
        template_str = "expect_queried_columnlist_to_have_no_duplicates"

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    examples = [
        {
            "data": [
                {
                    "dataset_name": "test",
                    "data": {
                        "col1": ["A", "A", "C", "C"],
                        "col2": ["1", "1", "3", "3"],
                        "col3": ["A", "B", "C", "D"],
                        "col4": ["A", "A", "C", "D"],
                    },
                },
            ],
            "tests": [
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"template_dict": {"column_list": "col1, col2, col4"}},
                    "out": {"success": False},
                    "only_for": ["sqlite", "spark"],
                },
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"template_dict": {"column_list": "col2, col3"}},
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

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@InesPhilipsen"],
    }


if __name__ == "__main__":
    ExpectQueriedColumnlistToHaveNoDuplicates().print_diagnostic_checklist()
