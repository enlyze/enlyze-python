import json
from functools import cache
from http import HTTPStatus
from typing import Any, Iterator, Type, TypeVar

import httpx
from pydantic import BaseModel, ValidationError

from enlyze._version import VERSION
from enlyze.auth import TokenAuth
from enlyze.constants import HTTPX_TIMEOUT, PLATFORM_API_SUB_PATH, USER_AGENT
from enlyze.errors import EnlyzeError, InvalidTokenError

from .models import PlatformApiModel

T = TypeVar("T", bound=PlatformApiModel)

USER_AGENT_NAME_VERSION_SEPARATOR = "/"


@cache
def _construct_user_agent(
    *, user_agent: str = USER_AGENT, version: str = VERSION
) -> str:
    return f"{user_agent}{USER_AGENT_NAME_VERSION_SEPARATOR}{version}"


class _Metadata(BaseModel):
    next_cursor: str | None


class _PaginatedResponse(BaseModel):
    metadata: _Metadata
    data: list[dict[str, Any]] | dict[str, Any]


class PlatformApiClient:
    """API Client class encapsulating all interaction with the ENLYZE platform

    :param token: API token for the ENLYZE platform
    :param base_url: Base URL of the ENLYZE platform
    :param timeout: Global timeout for all HTTP requests sent to the ENLYZE platform

    """

    def __init__(
        self,
        *,
        token: str,
        base_url: str | httpx.URL,
        timeout: float = HTTPX_TIMEOUT,
    ):
        self._client = httpx.Client(
            auth=TokenAuth(token),
            base_url=httpx.URL(base_url).join(PLATFORM_API_SUB_PATH),
            timeout=timeout,
            headers={"user-agent": _construct_user_agent()},
        )

    @cache
    def _full_url(self, api_path: str) -> str:
        """Construct full URL from relative URL"""
        return str(self._client.build_request("", api_path).url)

    def get(self, api_path: str, **kwargs: Any) -> Any:
        """Wraps :meth:`httpx.Client.get` with defensive error handling

        :param api_path: Relative URL path inside the API name space (or a full URL)

        :returns: JSON payload of the response as Python object

        :raises: :exc:`~enlyze.errors.EnlyzeError` on request failure
        :raises: :exc:`~enlyze.errors.EnlyzeError` on non-2xx status code
        :raises: :exc:`~enlyze.errors.EnlyzeError` on non-JSON payload

        """
        try:
            response = self._client.get(api_path, **kwargs)
        except Exception as e:
            raise EnlyzeError(
                "Couldn't read from the ENLYZE platform API "
                f"(GET {self._full_url(api_path)})",
            ) from e

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (
                HTTPStatus.UNAUTHORIZED,
                HTTPStatus.FORBIDDEN,
            ):
                raise InvalidTokenError
            else:
                raise EnlyzeError(
                    f"ENLYZE platform API returned error {response.status_code}"
                    f" (GET {self._full_url(api_path)})"
                ) from e

        try:
            return response.json()
        except json.JSONDecodeError as e:
            raise EnlyzeError(
                "ENLYZE platform API didn't return a valid JSON object "
                f"(GET {self._full_url(api_path)})",
            ) from e

    def get_paginated(
        self, api_path: str, model: Type[T], **kwargs: Any
    ) -> Iterator[T]:
        """Retrieve objects from a paginated ENLYZE platform API endpoint via HTTP GET

        :param api_path: Relative URL path inside the ENLYZE platform API
        :param model: API response model class derived from
            :class:`~enlyze.api_client.models.PlatformApiModel`

        :returns: Instances of ``model`` retrieved from the ``api_path`` endpoint

        :raises: :exc:`~enlyze.errors.EnlyzeError` on invalid pagination schema
        :raises: :exc:`~enlyze.errors.EnlyzeError` on invalid data schema
        :raises: see :py:meth:`get` for more errors raised by this method

        """

        params = kwargs.pop("params", {})

        while True:
            response_body = self.get(api_path, params=params, **kwargs)

            try:
                paginated_response = _PaginatedResponse.model_validate(response_body)
            except ValidationError as e:
                raise EnlyzeError(
                    f"Paginated response expected (GET {self._full_url(api_path)})"
                ) from e

            page_data = paginated_response.data
            if not page_data:
                break

            page_data = page_data if isinstance(page_data, list) else [page_data]

            for elem in page_data:
                try:
                    yield model.model_validate(elem)
                except ValidationError as e:
                    raise EnlyzeError(
                        f"ENLYZE platform API returned an unparsable {model.__name__} "
                        f"object (GET {self._full_url(api_path)})"
                    ) from e

            next_cursor = paginated_response.metadata.next_cursor
            if next_cursor is None:
                break

            params = {**params, "cursor": next_cursor}
