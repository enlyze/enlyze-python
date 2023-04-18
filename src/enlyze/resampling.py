import enlyze.models as user_models
from enlyze.validators import validate_resampling_method_for_data_type


def convert_to_variable_with_resampling_method(
    variable: user_models.Variable, resampling_method: user_models.ResamplingMethod
) -> user_models.VariableWithResamplingMethod:
    """Convert a :class:`~enlyze.models.Variable` to a :class:`~enlyze.models.VariableWithResamplingMethod`. # noqa: E501

    :param variable: The variable to convert.
    :param resampling_method: The resampling method to set on the variable.

    :raises: |resampling-error|

    :returns: A variable with resampling method.
    :rtype: :class:`~enlyze.models.VariableWithResamplingMethod`.

    """

    validate_resampling_method_for_data_type(resampling_method, variable.data_type)

    return user_models.VariableWithResamplingMethod(
        uuid=variable.uuid,
        display_name=variable.display_name,
        unit=variable.unit,
        data_type=variable.data_type,
        appliance=variable.appliance,
        resampling_method=resampling_method,
    )
