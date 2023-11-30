#: Base URL of the ENLYZE platform.
ENLYZE_BASE_URL = "https://app.enlyze.com"

#: URL sub-path where the Timeseries API is deployed on the ENLYZE platform.
TIMESERIES_API_SUB_PATH = "api/timeseries/v1/"

#: URL sub-path where the Production Runs API is deployed on the ENLYZE platform.
PRODUCTION_RUNS_API_SUB_PATH = "api/production-runs/v1/"

#: HTTP timeout for requests to the Timeseries API.
#:
#: Reference: https://www.python-httpx.org/advanced/#timeout-configuration
HTTPX_TIMEOUT = 30.0

#: The separator to use when to separate the variable UUID and the resampling method
#: when querying timeseries data.
VARIABLE_UUID_AND_RESAMPLING_METHOD_SEPARATOR = "||"

#: The minimum allowed resampling interval when resampling timeseries data.
MINIMUM_RESAMPLING_INTERVAL = 10
