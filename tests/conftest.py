import os
from datetime import datetime, timezone
from unittest.mock import patch

import hypothesis
import pytest
from hypothesis import strategies as st

from enlyze.api_clients.base import ApiBaseModel

hypothesis.settings.register_profile("ci", deadline=None)
hypothesis.settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "default"))

datetime_today_until_now_strategy = st.datetimes(
    min_value=datetime.utcnow().replace(hour=0),
    max_value=datetime.utcnow(),
    timezones=st.just(timezone.utc),
)

datetime_before_today_strategy = st.datetimes(
    max_value=datetime.utcnow().replace(hour=0),
    timezones=st.just(timezone.utc),
)


@pytest.fixture
def auth_token():
    return "some-token"


@pytest.fixture
def string_model():
    with patch(
        "enlyze.api_clients.base.ApiBaseModel.parse_obj",
        side_effect=lambda o: str(o),
    ):
        yield ApiBaseModel
