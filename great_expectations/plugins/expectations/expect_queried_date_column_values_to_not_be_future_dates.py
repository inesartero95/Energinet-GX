"""
This is an example of a Custom QueryExpectation.
For detailed information on QueryExpectations, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""
from os import environ
from sys import executable
from typing import Any, List, Optional, Union

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
    RenderedBulletListContent,
    RenderedGraphContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
)
from great_expectations.render.util import substitute_none_for_missing

environ["PYSPARK_PYTHON"] = executable
environ["PYSPARK_DRIVER_PYTHON"] = executable


class ExpectQueriedDateColumnValuesToNotBeFutureDates(QueryExpectation):
    """Expect the no date to be in the future."""

    metric_dependencies = ("query.column",)

    query = """
            SELECT * FROM {active_batch} WHERE {col} > GETDATE()
            """

    success_keys = ("column", "query")

    domain_keys = ("batch_id", "row_condition", "condition_parser")

    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
        "meta": None,
        "column": None,
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
        query_result = metrics.get("query.column")
        query_result = dict([element.values() for element in query_result])
        success = not query_result

        unexpected_values = [(date, v) for date, v in query_result.items()]

        if success == True:
          return {
              "success": success,
              "result": {
                  "observed_value": "100% not future dates",
              },
          }
        else:
              return {
              "success": success,
              "result": {
                  "observed_value": f"unexpected_list: {unexpected_values}",
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
                "column",
                "query",
                "condition_parser",
                "row_condition_parser",
            ],
        )

        # build string template
        template_str = "expect_queried_date_column_values_to_not_be_future_dates"
        # if include_column_name:
        #     template_str = "$column " + template_str
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

    examples = []

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["InesPhilipsen"],
    }


if __name__ == "__main__":
    ExpectQueriedDateColumnValuesToNotBeFutureDates().print_diagnostic_checklist()
