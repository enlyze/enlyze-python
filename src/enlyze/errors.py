class EnlyzeError(Exception):
    """Base class for all errors raised by the Timeseries SDK.

    In many situations this error will be raised with a concrete message describing the
    issue.

    """


class InvalidTokenError(EnlyzeError):
    """The ENLYZE platform token is not valid

    Possible reasons:

    - Invalid format (e.g. empty string)
    - Token has expired
    - Typo (e.g. check for excess whitespace)
    - Note: you can **not** use your password as a token

    """


class ResamplingValidationError(EnlyzeError):
    """Invalid request for resampling

    Timeseries data could not be requested from the ENLYZE platform. This may happen
    when a resampling method is incompatible with the variable data type or an invalid
    resampling interval is specified.

    """


class DuplicateDisplayNameError(EnlyzeError):
    """Variables with duplicate display names

    Resolving variable UUIDs to display names would result in ambiguity because multiple
    variables have the same display name. You should either fix the duplicate variable
    display names via the ENLYZE App or don't request them at the same time.

    """
