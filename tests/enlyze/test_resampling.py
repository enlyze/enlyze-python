from dataclasses import asdict

import pytest
from hypothesis import given
from hypothesis import strategies as st

from enlyze.errors import ResamplingValidationError
from enlyze.models import ResamplingMethod, Variable, VariableDataType
from enlyze.resampling import convert_to_variable_with_resampling_method
from enlyze.validators import VARIABLE_ARRAY_DATA_TYPES


@given(
    variable=st.builds(
        Variable,
        data_type=st.sampled_from((VariableDataType.FLOAT, VariableDataType.INTEGER)),
    ),
    resampling_method=st.sampled_from(ResamplingMethod),
)
def test_convert_to_variable_with_resampling_method(variable, resampling_method):
    variable_with_resampling_method = convert_to_variable_with_resampling_method(
        variable, resampling_method
    )

    variable_with_resampling_method_dict = asdict(variable_with_resampling_method)
    variable_resampling_method = variable_with_resampling_method_dict.pop(
        "resampling_method"
    )

    assert asdict(variable) == variable_with_resampling_method_dict
    assert variable_resampling_method == resampling_method


@given(
    variable=st.builds(
        Variable,
        data_type=st.sampled_from((VariableDataType.BOOLEAN, VariableDataType.STRING)),
    ),
    resampling_method=st.sampled_from(
        (
            ResamplingMethod.FIRST,
            ResamplingMethod.LAST,
            ResamplingMethod.MAX,
            ResamplingMethod.MIN,
            ResamplingMethod.COUNT,
        )
    ),
)
def test_convert_to_variable_with_resampling_method_boolean_or_string(
    variable, resampling_method
):
    variable_with_resampling_method = convert_to_variable_with_resampling_method(
        variable, resampling_method
    )

    variable_with_resampling_method_dict = asdict(variable_with_resampling_method)
    variable_resampling_method = variable_with_resampling_method_dict.pop(
        "resampling_method"
    )

    assert asdict(variable) == variable_with_resampling_method_dict
    assert variable_resampling_method == resampling_method


@given(
    variable=st.builds(
        Variable,
        data_type=st.sampled_from((VariableDataType.BOOLEAN, VariableDataType.STRING)),
    ),
    resampling_method=st.sampled_from(
        (
            ResamplingMethod.AVG,
            ResamplingMethod.SUM,
            ResamplingMethod.MEDIAN,
        )
    ),
)
def test_convert_to_variable_with_resampling_method_boolean_or_string_raises(
    variable, resampling_method
):
    with pytest.raises(ResamplingValidationError):
        convert_to_variable_with_resampling_method(variable, resampling_method)


@given(
    variable=st.builds(
        Variable,
        data_type=st.sampled_from(VARIABLE_ARRAY_DATA_TYPES),
    ),
    resampling_method=st.sampled_from(ResamplingMethod),
)
def test_convert_to_variable_with_resampling_method_array_raises(
    variable, resampling_method
):
    with pytest.raises(ResamplingValidationError):
        convert_to_variable_with_resampling_method(variable, resampling_method)
