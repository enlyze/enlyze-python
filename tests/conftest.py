import os

import hypothesis

hypothesis.settings.register_profile("ci", deadline=None)
hypothesis.settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "default"))
