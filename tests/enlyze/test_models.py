from dataclasses import replace
from datetime import datetime
from uuid import uuid4

import hypothesis.strategies as st
import pytest
from hypothesis import given

from enlyze.errors import DuplicateDisplayNameError
from enlyze.models import ProductionRun, ProductionRuns, TimeseriesData, Variable


@given(runs=st.lists(st.from_type(ProductionRun), max_size=10))
def test_production_runs_to_dataframe(runs: list[ProductionRun]):
    runs = ProductionRuns(runs)
    runs.to_dataframe()


@given(run=st.from_type(ProductionRun))
def test_production_runs_to_dataframe_no_empty_columns_for_optional_dataclasses(
    run: ProductionRun,
):
    df = ProductionRuns(
        [
            replace(
                run,
                average_throughput=None,
                quantity_total=None,
            )
        ]
    ).to_dataframe()

    assert "quantity_total" not in df.columns
    assert "average_throughput" in df.columns


@given(variable=st.builds(Variable, display_name=st.text(min_size=1)))
def test_timeseries_data_duplicate_display_names(variable):

    variable_duplicate = replace(variable, uuid=uuid4())
    variables = [variable, variable_duplicate]

    data = TimeseriesData(
        start=datetime.now(),
        end=datetime.now(),
        variables=variables,
        _columns=["time", *[str(v.uuid) for v in variables]],
        _records=[],
    )

    with pytest.raises(DuplicateDisplayNameError):
        data.to_dataframe(use_display_names=True)
