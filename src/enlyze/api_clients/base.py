import json
from abc import ABC, abstractmethod
from collections.abc import Iterator
from functools import cache
from http import HTTPStatus
from typing import Any, Generic, TypeVar

import httpx
from pydantic import BaseModel, ValidationError

from enlyze.auth import TokenAuth
from enlyze.constants import HTTPX_TIMEOUT
from enlyze.errors import EnlyzeError, InvalidTokenError


class ApiBaseModel(BaseModel):
    """Base class for ENLYZE platform API object models using pydantic

    All objects received from ENLYZE platform APIs are passed into models that derive
    from this class and thus use pydantic for schema definition and validation.

    """


class PaginatedResponseBaseModel(BaseModel):
    """Base class for paginated ENLYZE platform API responses using pydantic."""

    data: Any


#: TypeVar("M", bound=ApiBaseModel): Type variable serving as a parameter
# for API response model classes.
M = TypeVar("M", bound=ApiBaseModel)


#: TypeVar("R", bound=PaginatedResponseBaseModel) Type variable serving as a parameter
# for paginated response models.
R = TypeVar("R", bound=PaginatedResponseBaseModel)


class ApiBaseClient(ABC, Generic[R]):
    """Client base class encapsulating all interaction with all ENLYZE platform APIs.

    :param token: API token for the ENLYZE platform
    :param base_url: Base URL of the ENLYZE platform
    :param timeout: Global timeout for HTTP requests sent to the ENLYZE platform APIs

    """

    PaginatedResponseModel: type[R]

    def __init__(
        self,
        *,
        token: str,
        base_url: str | httpx.URL,
        timeout: float = HTTPX_TIMEOUT,
    ):
        self._client = httpx.Client(
            auth=TokenAuth(token),
            base_url=httpx.URL(base_url),
            timeout=timeout,
        )

    @cache
    def _full_url(self, api_path: str) -> str:
        """Construct full URL from relative URL"""
        return str(self._client.build_request("", api_path).url)

    def get(self, api_path: str, **kwargs: Any) -> Any:
        """Wraps :meth:`httpx.Client.get` with defensive error handling

        :param api_path: Relative URL path inside the API name space (or a full URL)

        :raises: :exc:`~enlyze.errors.EnlyzeError` on request failure

        :raises: :exc:`~enlyze.errors.EnlyzeError` on non-2xx status code

        :raises: :exc:`~enlyze.errors.EnlyzeError` on non-JSON payload

        :returns: JSON payload of the response as Python object

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

    def _transform_paginated_response_data(self, data: Any) -> Any:
        """Transform paginated response data. Returns ``data`` by default.

        :param data: Response data from a paginated response

        :returns: An iterable of transformed data

        """
        return data

    @abstractmethod
    def _has_more(self, paginated_response: R) -> bool:
        """Indicates there is more data to fetch from the server.

        :param paginated_response: A paginated response model deriving from
            :class:`PaginatedResponseBaseModel`.

        """

    @abstractmethod
    def _next_page_call_args(
        self,
        *,
        url: str,
        params: dict[str, Any],
        paginated_response: R,
        **kwargs: Any,
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        r"""Compute call arguments for the next page.

        :param url: The URL used to fetch the current page
        :param params: URL query parameters of the current page
        :param paginated_response: A paginated response model deriving from
            :class:`~enlyze.api_clients.base.PaginatedResponseBaseModel`
        :param \**kwargs: Keyword arguments passed into
            :py:meth:`~enlyze.api_clients.base.ApiBaseClient.get_paginated`

        :returns: A tuple of comprised of the URL, query parameters and keyword
            arguments to fetch the next page

        """

    def get_paginated(
        self, api_path: str, model: type[M], **kwargs: Any
    ) -> Iterator[M]:
        """Retrieve objects from a paginated ENLYZE platform API endpoint via HTTP GET.

        To add pagination capabilities to an API client deriving from this class, two
        abstract methods need to be implemented,
        :py:meth:`~enlyze.api_clients.base.ApiBaseClient._has_more` and
        :py:meth:`~enlyze.api_clients.base.ApiBaseClient._next_page_call_args`.
        Optionally, API clients may transform page data by overriding
        :py:meth:`~enlyze.api_clients.base.ApiBaseClient._transform_paginated_response_data`,
        which by default returns the unmodified page data.

        :param api_path: Relative URL path inside the API name space
        :param model: API response model class deriving from
            :class:`~enlyze.api_clients.base.ApiBaseModel`

        :raises: :exc:`~enlyze.errors.EnlyzeError` on invalid pagination schema

        :raises: :exc:`~enlyze.errors.EnlyzeError` on invalid data schema

        :raises: see :py:meth:`get` for more errors raised by this method

        :returns: Instances of ``model`` retrieved from the ``api_path`` endpoint

        """

        url = api_path
        params = kwargs.pop("params", {})

        while True:
            response_body = self.get(url, params=params, **kwargs)
            try:
                paginated_response = self.PaginatedResponseModel.model_validate(
                    response_body
                )
            except ValidationError as e:
                raise EnlyzeError(
                    f"Paginated response expected (GET {self._full_url(url)})"
                ) from e

            page_data = paginated_response.data
            if not page_data:
                break

            page_data = self._transform_paginated_response_data(page_data)

            for elem in page_data:
                try:
                    yield model.model_validate(elem)
                except ValidationError as e:
                    raise EnlyzeError(
                        f"ENLYZE platform API returned an unparsable {model.__name__} "
                        f"object (GET {self._full_url(api_path)})"
                    ) from e
            if not self._has_more(paginated_response):
                break

            url, params, kwargs = self._next_page_call_args(
                url=url,
                params=params,
                paginated_response=paginated_response,
                **kwargs,
            )
