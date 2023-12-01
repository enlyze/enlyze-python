from typing import Any, Tuple

import httpx
from pydantic import AnyUrl

from enlyze.api_clients.base import ApiBaseClient, PaginatedResponseBaseModel
from enlyze.constants import TIMESERIES_API_SUB_PATH


class _PaginatedResponse(PaginatedResponseBaseModel):
    next: AnyUrl | None
    data: list[Any] | dict[str, Any]


class TimeseriesApiClient(ApiBaseClient[_PaginatedResponse]):
    """Client class encapsulating all interaction with the Timeseries API

    :param token: API token for the ENLYZE platform
    :param base_url: Base URL of the ENLYZE platform
    :param timeout: Global timeout for all HTTP requests sent to the Timeseries API

    """

    PaginatedResponseModel = _PaginatedResponse

    def __init__(
        self,
        *,
        token: str,
        base_url: str | httpx.URL,
        **kwargs: Any,
    ):
        super().__init__(
            token=token,
            base_url=httpx.URL(base_url).join(TIMESERIES_API_SUB_PATH),
            **kwargs,
        )

    def _transform_paginated_response_data(
        self, paginated_response_data: list[Any] | dict[str, Any]
    ) -> list[dict[str, Any]]:
        # The timeseries endpoint's response data field is a mapping.
        # Because get_paginated assumes the ``data`` field to be a list,
        # we wrap it into a list.
        return (
            paginated_response_data
            if isinstance(paginated_response_data, list)
            else [paginated_response_data]
        )

    def _has_more(self, paginated_response: _PaginatedResponse) -> bool:
        return paginated_response.next is not None

    def _next_page_call_args(
        self,
        *,
        url: str,
        params: dict[str, Any],
        paginated_response: _PaginatedResponse,
        **kwargs: Any,
    ) -> Tuple[str, dict[str, Any], dict[str, Any]]:
        next_url = str(paginated_response.next)
        return (next_url, params, kwargs)
