from typing import Any

import httpx
from pydantic import BaseModel

from enlyze.api_clients.base import ApiBaseClient, PaginatedResponseBaseModel
from enlyze.constants import PRODUCTION_RUNS_API_SUB_PATH


class _Metadata(BaseModel):
    next_cursor: int | None
    has_more: bool


class _PaginatedResponse(PaginatedResponseBaseModel):
    metadata: _Metadata
    data: list[dict[str, Any]]


class ProductionRunsApiClient(ApiBaseClient[_PaginatedResponse]):
    """Client class encapsulating all interaction with the Production Runs API

    :param token: API token for the ENLYZE platform
    :param base_url: Base URL of the ENLYZE platform
    :param timeout: Global timeout for all HTTP requests sent to the Production Runs API

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
            base_url=httpx.URL(base_url).join(PRODUCTION_RUNS_API_SUB_PATH),
            **kwargs,
        )

    def _has_more(self, paginated_response: _PaginatedResponse) -> bool:
        return paginated_response.metadata.has_more

    def _next_page_call_args(
        self,
        url: str,
        params: dict[str, Any],
        paginated_response: _PaginatedResponse,
        **kwargs: Any,
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        next_params = {**params, "cursor": paginated_response.metadata.next_cursor}
        return (url, next_params, kwargs)
