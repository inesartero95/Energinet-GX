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
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
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


class ExpectQueriedDateColumnToHaveNoDaysMissing(QueryExpectation):
    """Expect that there are no missing days between the most recent observation
    and the oldest observation"""

    metric_dependencies = ("query.column",)

    query = """
            SELECT y.TimeStamp, DATEDIFF(DAY,y.[LagTime],y.[TimeStamp]) AS [Diff]
            FROM(
            SELECT x.[TimeStamp],  LAG(x.[TimeStamp],1, x.[TimeStamp]) OVER(ORDER BY x.[TimeStamp]) AS [LagTime]
            FROM (
            SELECT DISTINCT dateadd(day, datediff(day, 0, {col}), 0) AS [TimeStamp] FROM {active_batch}
            ) x ) y
            ORDER BY TimeStamp DESC
            """

    success_keys = ("column", "value", "query")

    domain_keys = ("batch_id", "row_condition", "condition_parser")

    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
        "meta": None,
        "column": None,
        "value": None,
        "query": query,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
        value = configuration["kwargs"].get("value")
        try:
            assert value is not None, "'value' must be specified"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        metrics = convert_to_json_serializable(data=metrics)
        value = configuration["kwargs"].get("value")
        query_result = metrics.get("query.column")
        query_result = dict([element.values() for element in query_result])
        success = all(q <= value for q in query_result.values())

        unexpected_values = [(date, v) for date, v in query_result.items() if v > value]

        # how many dates should there be
        n = sum(query_result.values())
        
        if len(unexpected_values) == 0:
            unexpected_percentage = 0
        else:
            unexpected_percentage = len(unexpected_values) / n * 100

        return {
            "success": success,
            "result": { 
                "observed_value": f"\u2248 {round(unexpected_percentage, 5)} % unexpected. \n unexpected_list: {unexpected_values}", 
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
                "value",
                "query",
                "condition_parser",
                "row_condition_parser",
            ],
        )

        # build string template
        template_str = f"expect_queried_date_column_to_have_no_days_missing \n Expect all dates from {params['column']} to have no missing observations"
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
    ExpectQueriedDateColumnToHaveNoDaysMissing().print_diagnostic_checklist()
