import os
from datetime import datetime, timedelta, timezone

import hypothesis
import pytest
from hypothesis import strategies as st

hypothesis.settings.register_profile("ci", deadline=None)
hypothesis.settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "default"))

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

DATETIME_TODAY_MIDNIGHT = datetime.now().replace(
    hour=0,
    minute=0,
    second=0,
    microsecond=0,
)

datetime_today_until_now_strategy = st.datetimes(
    min_value=DATETIME_TODAY_MIDNIGHT,
    max_value=datetime.now(),
    timezones=st.just(timezone.utc),
)

datetime_before_today_strategy = st.datetimes(
    max_value=DATETIME_TODAY_MIDNIGHT - timedelta(microseconds=1),
    min_value=datetime(1970, 1, 1, 12, 0, 0),
    timezones=st.just(timezone.utc),
)


@pytest.fixture
def auth_token():
    return "some-token"
