from contextlib import nullcontext as does_not_raise
from datetime import datetime, timedelta
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st

from enlyze.errors import EnlyzeError, ResamplingValidationError
from enlyze.models import Variable, VariableWithResamplingMethod
from enlyze.validators import (
    validate_resampling_interval,
    validate_timeseries_arguments,
)
from tests.conftest import (
    datetime_before_today_strategy,
    datetime_today_until_now_strategy,
)

variable_strategy = st.one_of(
    st.builds(Variable), st.builds(VariableWithResamplingMethod)
)


class TestValidateTimeseriesArguments:
    @given(
        start=datetime_before_today_strategy,
        end=datetime_today_until_now_strategy,
        variable=variable_strategy,
    )
    def test_validate_timeseries_arguments(self, start, end, variable):
        start, end, appliance_uuid = validate_timeseries_arguments(
            start, end, [variable]
        )
        assert start
        assert end
        assert UUID(appliance_uuid)

    @given(variable=variable_strategy)
    def test_validate_start_must_be_earlier_than_end(self, variable):
        end = datetime.now()
        start = end + timedelta(days=1)
        with pytest.raises(EnlyzeError):
            validate_timeseries_arguments(start, end, [variable])

    @given(
        start=datetime_before_today_strategy,
        end=datetime_today_until_now_strategy,
    )
    def test_empty_variables(self, start, end):
        with pytest.raises(EnlyzeError):
            validate_timeseries_arguments(start, end, [])

    @given(
        start=datetime_before_today_strategy,
        end=datetime_today_until_now_strategy,
        variable1=variable_strategy,
        variable2=variable_strategy,
    )
    def test_variables_with_different_appliance(self, start, end, variable1, variable2):
        with pytest.raises(EnlyzeError):
            validate_timeseries_arguments(start, end, [variable1, variable2])


@pytest.mark.parametrize(
    "resampling_interval,expectation",
    [
        (10, does_not_raise()),
        (13, does_not_raise()),
        (15, does_not_raise()),
        (20, does_not_raise()),
        (-5, pytest.raises(ResamplingValidationError)),
        (0, pytest.raises(ResamplingValidationError)),
        (1, pytest.raises(ResamplingValidationError)),
        (5, pytest.raises(ResamplingValidationError)),
    ],
)
def test_validate_resampling_interval(resampling_interval, expectation):
    with expectation:
        validate_resampling_interval(resampling_interval)
