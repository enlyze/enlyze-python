import enlyze.models as user_models


def convert_to_variable_with_resampling_method(
    variable: user_models.Variable, resampling_method: user_models.ResamplingMethod
) -> user_models.VariableWithResamplingMethod:
    """Convert a :class:`~enlyze.models.Variable` to a
    :class:`~enlyze.models.VariableWithResamplingMethod`.

    :param variable: The variable to convert.
    :type variable: :class:`~enlyze.models.Variable`

    :param resampling_method: The resampling method to set on the variable.
    :type resampling_method: :class:`~enlyze.models.ResamplingMethod`

    :returns: A variable with resampling method.
    :rtype: :class:`~enlyze.models.VariableWithResamplingMethod`.
    """

    return user_models.VariableWithResamplingMethod(
        uuid=variable.uuid,
        display_name=variable.display_name,
        unit=variable.unit,
        data_type=variable.data_type,
        appliance=variable.appliance,
        resampling_method=resampling_method,
    )
