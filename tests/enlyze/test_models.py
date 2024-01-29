from dataclasses import replace
from datetime import datetime

import hypothesis.strategies as st
from hypothesis import given

from enlyze.models import ProductionRun, ProductionRuns, _get_optional_dataclass_fields

# https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timestamp-limitations
PANDAS_MIN_DATETIME = datetime(1677, 9, 21, 0, 12, 44)
PANDAS_MAX_DATETIME = datetime(2262, 4, 11, 23, 47, 16)

# https://github.com/python/cpython/issues/94414
WINDOWS_MIN_DATETIME = datetime(1970, 1, 2, 1, 0, 0)
WINDOWS_MAX_DATETIME = datetime(3001, 1, 19, 7, 59, 59)

st.register_type_strategy(
    datetime,
    st.datetimes(
        min_value=max(PANDAS_MIN_DATETIME, WINDOWS_MIN_DATETIME),
        max_value=min(PANDAS_MAX_DATETIME, WINDOWS_MAX_DATETIME),
    ),
)


@given(run=st.from_type(ProductionRun))
def test_production_run_to_dict_exclude_unset_objects(run: ProductionRun):
    run = replace(run, quality=None)
    assert "quality" in run.to_dict(exclude_unset_objects=False)
    assert "quality" not in run.to_dict(exclude_unset_objects=True)


# @given(runs=st.lists(st.from_type(ProductionRun), max_size=10))
# def test_production_runs_to_dataframe(runs: list[ProductionRun]):
#     runs = ProductionRuns(runs)
#     df = runs.to_dataframe()
#     assert not set(df) & _get_optional_dataclass_fields(ProductionRun)
