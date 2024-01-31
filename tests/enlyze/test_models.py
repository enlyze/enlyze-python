from dataclasses import replace

import hypothesis.strategies as st
from hypothesis import given

from enlyze.models import ProductionRun, ProductionRuns


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
