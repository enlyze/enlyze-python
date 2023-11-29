from contextlib import nullcontext as does_not_raise
from datetime import datetime, timedelta
from uuid import UUID

import pytest
from hypothesis import example, given
from hypothesis import strategies as st

from enlyze.errors import EnlyzeError, ResamplingValidationError
from enlyze.models import ResamplingMethod, Variable, VariableDataType
from enlyze.validators import (
    VARIABLE_ARRAY_DATA_TYPES,
    _ensure_datetime_aware,
    validate_datetime,
    validate_resampling_interval,
    validate_resampling_method_for_data_type,
    validate_start_and_end,
    validate_timeseries_arguments,
)
from tests.conftest import (
    datetime_before_today_strategy,
    datetime_today_until_now_strategy,
)

VARIABLE_STRATEGY = st.builds(Variable)


@given(
    dt=st.datetimes(
        max_value=datetime.now(),
        timezones=st.timezones(),
    )
)
def test_ensure_datetime_aware(dt):
    datetime_with_timezone = _ensure_datetime_aware(dt)
    assert datetime_with_timezone.utcoffset() is not None


@given(
    dt=st.datetimes(
        max_value=datetime.now(),
        timezones=st.timezones(),
    )
)
@example(dt=datetime(2021, 1, 1, 0, 0, 0, 0))
def test_validate_datetime(dt):
    validate_datetime(dt)


class TestValidateStartAndEnd:
    @given(
        start=datetime_before_today_strategy,
        end=datetime_today_until_now_strategy,
    )
    def test_validate_timeseries_arguments(self, start, end):
        start, end = validate_start_and_end(start, end)

    def test_validate_start_must_be_earlier_than_end(self):
        end = datetime.now()
        start = end + timedelta(days=1)
        with pytest.raises(EnlyzeError):
            validate_start_and_end(start, end)


class TestValidateTimeseriesArguments:
    @given(
        start=datetime_before_today_strategy,
        end=datetime_today_until_now_strategy,
        variable=VARIABLE_STRATEGY,
    )
    def test_validate_timeseries_arguments(self, start, end, variable):
        start, end, appliance_uuid = validate_timeseries_arguments(
            start, end, [variable]
        )
        assert start
        assert end
        assert UUID(appliance_uuid)

    @given(variable=VARIABLE_STRATEGY)
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
        variable1=VARIABLE_STRATEGY,
        variable2=VARIABLE_STRATEGY,
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


@pytest.mark.parametrize(
    "resampling_method_strategy,data_type_strategy,expectation",
    [
        (
            st.sampled_from(ResamplingMethod),
            st.sampled_from(VARIABLE_ARRAY_DATA_TYPES),
            pytest.raises(ResamplingValidationError),
        ),
        (
            st.sampled_from(
                (ResamplingMethod.AVG, ResamplingMethod.SUM, ResamplingMethod.MEDIAN)
            ),
            st.sampled_from((VariableDataType.BOOLEAN, VariableDataType.STRING)),
            pytest.raises(ResamplingValidationError),
        ),
        (
            st.sampled_from(ResamplingMethod),
            st.sampled_from(
                (
                    VariableDataType.INTEGER,
                    VariableDataType.FLOAT,
                )
            ),
            does_not_raise(),
        ),
        (
            st.sampled_from(
                (
                    ResamplingMethod.FIRST,
                    ResamplingMethod.LAST,
                    ResamplingMethod.MIN,
                    ResamplingMethod.MAX,
                    ResamplingMethod.COUNT,
                )
            ),
            st.sampled_from(
                (
                    VariableDataType.BOOLEAN,
                    VariableDataType.STRING,
                )
            ),
            does_not_raise(),
        ),
    ],
)
@given(
    data_strategy=st.data(),
)
def test_validate_resampling_method_for_data_type(
    resampling_method_strategy, data_type_strategy, expectation, data_strategy
):
    resampling_method = data_strategy.draw(resampling_method_strategy)
    data_type = data_strategy.draw(data_type_strategy)

    with expectation:
        assert (
            validate_resampling_method_for_data_type(resampling_method, data_type)
            is None
        )
