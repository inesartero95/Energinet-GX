from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)


class ColumnAggregateMatchesSomeCriteria(ColumnAggregateMetricProvider):

    # This is the id string that will be used to reference your Metric.
    metric_name = "column.custom_max"

    # This method implements the core logic for the PandasExecutionEngine
    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        raise NotImplementedError

# This class defines the Expectation itself
class ExpectColumnAggregateToMatchSomeCriteria(ColumnExpectation):
    """TODO: add a docstring here"""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = []

    metric_dependencies = ("column.custom_max",)

    success_keys = ("min_value", "strict_min", "max_value", "strict_max")

    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

    # This method performs a validation of your metrics against your success keys, 
    # returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        """Validates the given data against the set minimum 
        and maximum value thresholds for the column max"""
        column_max = metrics["column.custom_max"]

        # Obtaining components needed for validation
        min_value = self.get_success_kwargs(configuration).get("min_value")
        strict_min = self.get_success_kwargs(configuration).get("strict_min")
        max_value = self.get_success_kwargs(configuration).get("max_value")
        strict_max = self.get_success_kwargs(configuration).get("strict_max")

        # Checking if mean lies between thresholds
        if min_value is not None:
            if strict_min:
                above_min = column_max > min_value
            else:
                above_min = column_max >= min_value
        else:
            above_min = True

        if max_value is not None:
            if strict_max:
                below_max = column_max < max_value
            else:
                below_max = column_max <= max_value
        else:
            below_max = True

        success = above_min and below_max

        return {"success": success, "result": {"observed_value": column_max}}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all 
            "@your_name_here",  
        ],
    }


if __name__ == "__main__":
    ExpectColumnAggregateToMatchSomeCriteria().print_diagnostic_checklist()