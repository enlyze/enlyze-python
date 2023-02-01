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
