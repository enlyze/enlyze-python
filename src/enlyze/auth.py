from collections.abc import Generator

from httpx import Auth, Request, Response

from enlyze.errors import InvalidTokenError


class TokenAuth(Auth):
    """Token authentication scheme for use with ``httpx``

    :param token: API token for the ENLYZE platform

    """

    def __init__(self, token: str):
        if not isinstance(token, str):
            raise InvalidTokenError("Token must be a string")

        if not token:
            raise InvalidTokenError("Token must not be empty")

        self._auth_header = f"Token {token}"

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """Inject token into authorization header"""
        request.headers["Authorization"] = self._auth_header
        yield request
