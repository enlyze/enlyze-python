import json
from collections.abc import Iterator
from functools import cache
from http import HTTPStatus
from typing import Any, Type, TypeVar

import httpx
from pydantic import AnyUrl, BaseModel, ValidationError

from enlyze.auth import TokenAuth
from enlyze.constants import ENLYZE_BASE_URL, HTTPX_TIMEOUT, TIMESERIES_API_SUB_PATH
from enlyze.errors import EnlyzeError, InvalidTokenError
from enlyze.timeseries_api.models import TimeseriesApiModel

T = TypeVar("T", bound=TimeseriesApiModel)


class _PaginatedResponse(BaseModel):
    next: AnyUrl | None
    data: list[Any] | dict[str, Any]


class TimeseriesApiClient:
    """Client class encapsulating all interaction with the Timeseries API

    :param token: API token for the ENLYZE platform
    :param base_url: Base URL of the ENLYZE platform
    :param timeout: Global timeout for all HTTP requests sent to the Timeseries API

    """

    def __init__(
        self,
        token: str,
        *,
        base_url: str | httpx.URL = ENLYZE_BASE_URL,
        timeout: float = HTTPX_TIMEOUT,
    ):
        self._client = httpx.Client(
            auth=TokenAuth(token),
            base_url=httpx.URL(base_url).join(TIMESERIES_API_SUB_PATH),
            timeout=timeout,
        )

    @cache
    def _full_url(self, api_path: str) -> str:
        """Construct full URL from relative URL"""
        return str(self._client.build_request("", api_path).url)

    def get(self, api_path: str, **kwargs: Any) -> Any:
        """Wraps :meth:`httpx.Client.get` with defensive error handling

        :param api_path: Relative URL path inside the Timeseries API (or a full URL)

        :raises: :exc:`~enlyze.errors.EnlyzeError` on request failure

        :raises: :exc:`~enlyze.errors.EnlyzeError` on non-2xx status code

        :raises: :exc:`~enlyze.errors.EnlyzeError` on non-JSON payload

        :returns: JSON payload of the response as Python object

        """

        try:
            response = self._client.get(api_path, **kwargs)
        except Exception as e:
            raise EnlyzeError(
                "Couldn't read from the Timeseries API "
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
                    f"Timeseries API returned error {response.status_code}"
                    f" (GET {self._full_url(api_path)})"
                ) from e

        try:
            return response.json()
        except json.JSONDecodeError as e:
            raise EnlyzeError(
                "Timeseries API didn't return a valid JSON object "
                f"(GET {self._full_url(api_path)})",
            ) from e

    def get_paginated(
        self, api_path: str, model: Type[T], **kwargs: Any
    ) -> Iterator[T]:
        """Retrieve objects from paginated Timeseries API endpoint via HTTP GET

        :param api_path: Relative URL path inside the Timeseries API
        :param model: Class derived from
            :class:`~enlyze.timeseries_api.models.TimeseriesApiModel`

        :raises: :exc:`~enlyze.errors.EnlyzeError` on invalid pagination schema

        :raises: :exc:`~enlyze.errors.EnlyzeError` on invalid data schema

        :raises: see :py:meth:`get` for more errors raised by this method

        :returns: Instances of ``model`` retrieved from the ``api_path`` endpoint

        """

        url = api_path

        while True:
            response_body = self.get(url, **kwargs)

            try:
                paginated_response = _PaginatedResponse.parse_obj(response_body)
            except ValidationError as e:
                raise EnlyzeError(
                    f"Paginated response expected (GET {self._full_url(url)})"
                ) from e

            page_data = paginated_response.data
            if not page_data:
                break

            # if `data` is a list we assume there are multiple objects inside.
            # if `data` is a dict then we treat it as only one object
            page_data = page_data if isinstance(page_data, list) else [page_data]

            for elem in page_data:
                try:
                    yield model.parse_obj(elem)
                except ValidationError as e:
                    raise EnlyzeError(
                        f"Timeseries API returned an unparsable {model.__name__} "
                        f"object (GET {self._full_url(api_path)})"
                    ) from e

            if not paginated_response.next:
                break

            url = str(paginated_response.next)
