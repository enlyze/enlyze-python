#: Base URL of the ENLYZE platform.
ENLYZE_BASE_URL = "https://app.enlyze.com"

#: URL sub-path of the ENLYZE platform API.
PLATFORM_API_SUB_PATH = "api/v2/"

#: HTTP timeout for requests to the Timeseries API.
#:
#: Reference: https://www.python-httpx.org/advanced/timeouts/
HTTPX_TIMEOUT = 30.0

#: The separator to use when to separate the variable UUID and the resampling method
#: when querying timeseries data.
VARIABLE_UUID_AND_RESAMPLING_METHOD_SEPARATOR = "||"

#: The minimum allowed resampling interval when resampling timeseries data.
MINIMUM_RESAMPLING_INTERVAL = 10

#: The maximum number of variables that can be used in a single request when querying
#: timeseries data.
MAXIMUM_NUMBER_OF_VARIABLES_PER_TIMESERIES_REQUEST = 100

#: The user agent that the SDK identifies itself as when making HTTP requests
USER_AGENT = "enlyze-python"
