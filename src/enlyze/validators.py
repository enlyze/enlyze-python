import logging
from datetime import datetime, timezone
from typing import Sequence

import enlyze.models as user_models
from enlyze.constants import MINIMUM_RESAMPLING_INTERVAL
from enlyze.errors import EnlyzeError, ResamplingValidationError

VARIABLE_ARRAY_DATA_TYPES = (
    user_models.VariableDataType.ARRAY_BOOLEAN,
    user_models.VariableDataType.ARRAY_STRING,
    user_models.VariableDataType.ARRAY_INTEGER,
    user_models.VariableDataType.ARRAY_FLOAT,
)

_NAIVE_DATETIME_DISCOURAGED_LOG_MESSAGE = (
    "Passing naive datetime is discouraged, assuming local timezone."
)


def _ensure_datetime_aware(dt: datetime) -> datetime:
    """Make the returned datetime timezone aware.

    Naive datetime will be assumed to be in local timezone and then converted to aware
    datetime expressed in UTC.

    """
    return dt.astimezone(timezone.utc)


def validate_datetime(dt: datetime) -> datetime:
    if dt.utcoffset() is None:
        logging.warning(_NAIVE_DATETIME_DISCOURAGED_LOG_MESSAGE)

    return _ensure_datetime_aware(dt)


def validate_start_and_end(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    if start.utcoffset() is None or end.utcoffset() is None:
        logging.warning(_NAIVE_DATETIME_DISCOURAGED_LOG_MESSAGE)

    start = _ensure_datetime_aware(start)
    end = _ensure_datetime_aware(end)
    if start >= end:
        raise EnlyzeError("Start must be earlier than end.")
    return start, end


def validate_timeseries_arguments(
    start: datetime,
    end: datetime,
    variables: Sequence[user_models.Variable],
) -> tuple[datetime, datetime, str]:
    if not variables:
        raise EnlyzeError("Need to request at least one variable")

    start, end = validate_start_and_end(start, end)

    appliance_uuids = frozenset(v.appliance.uuid for v in variables)

    if len(appliance_uuids) != 1:
        raise EnlyzeError(
            "Cannot request timeseries data for more than one appliance per request."
        )

    return start, end, str(next(iter(appliance_uuids)))


def validate_resampling_interval(
    resampling_interval: int,
) -> None:
    if resampling_interval < MINIMUM_RESAMPLING_INTERVAL:
        raise ResamplingValidationError(
            "resampling_interval must be greater than or equal to"
            f" {MINIMUM_RESAMPLING_INTERVAL}, {resampling_interval=} provided."
        )


def validate_resampling_method_for_data_type(
    resampling_method: user_models.ResamplingMethod,
    data_type: user_models.VariableDataType,
) -> None:
    if data_type in VARIABLE_ARRAY_DATA_TYPES:
        raise ResamplingValidationError(
            f"Cannot resample {data_type=} as it is an array variable data type"
        )

    if data_type in (
        user_models.VariableDataType.BOOLEAN,
        user_models.VariableDataType.STRING,
    ) and resampling_method in (
        user_models.ResamplingMethod.SUM,
        user_models.ResamplingMethod.AVG,
        user_models.ResamplingMethod.MEDIAN,
    ):
        raise ResamplingValidationError(
            f"{data_type=} cannot be resampled with {resampling_method=}"
        )
