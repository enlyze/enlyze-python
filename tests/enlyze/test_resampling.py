from hypothesis import given
from hypothesis import strategies as st

from enlyze.models import ResamplingMethod, Variable
from enlyze.resampling import convert_to_variable_with_resampling_method


@given(
    variable=st.builds(Variable), resampling_method=st.sampled_from(ResamplingMethod)
)
def test_convert_to_variable_with_resampling_method(variable, resampling_method):
    variable_with_resampling_method = convert_to_variable_with_resampling_method(
        variable, resampling_method
    )

    assert variable_with_resampling_method.uuid == variable.uuid
    assert variable_with_resampling_method.display_name == variable.display_name
    assert variable_with_resampling_method.unit == variable.unit
    assert variable_with_resampling_method.data_type == variable.data_type
    assert variable_with_resampling_method.appliance == variable.appliance
    assert variable_with_resampling_method.resampling_method == resampling_method
