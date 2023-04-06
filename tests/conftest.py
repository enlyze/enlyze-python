import os
from datetime import datetime, timezone

import hypothesis
from hypothesis import strategies as st

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
