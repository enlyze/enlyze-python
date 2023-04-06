import logging
from datetime import datetime, timezone
from typing import Sequence

import enlyze.models as user_models
from enlyze.constants import MINIMUM_RESAMPLING_INTERVAL
from enlyze.errors import EnlyzeError, ResamplingValidationError


def _ensure_datetime_aware(dt: datetime) -> datetime:
    """Make the returned datetime timezone aware.

    Naive datetime will be assumed to be in local timezone and then converted to aware
    datetime expressed in UTC.

    """
    return dt.astimezone(timezone.utc)


def validate_timeseries_arguments(
    start: datetime,
    end: datetime,
    variables: Sequence[user_models.Variable]
    | Sequence[user_models.VariableWithResamplingMethod],
) -> tuple[datetime, datetime, str]:
    if not variables:
        raise EnlyzeError("Need to request at least one variable")

    if start.utcoffset() is None or end.utcoffset() is None:
        logging.warning(
            "Passing naive datetime is discouraged, assuming local timezone."
        )

    start = _ensure_datetime_aware(start)
    end = _ensure_datetime_aware(end)
    if start > end:
        raise EnlyzeError("Start must be earlier than end.")

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
