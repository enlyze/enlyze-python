import httpx
import pytest
import respx

from enlyze.api_clients.production_runs.client import (
    ProductionRunsApiClient,
    _Metadata,
    _PaginatedResponse,
)
from enlyze.constants import ENLYZE_BASE_URL, PRODUCTION_RUNS_API_SUB_PATH


@pytest.fixture
def metadata_last_page():
    return _Metadata(has_more=False, next_cursor=None)


@pytest.fixture
def metadata_next_page():
    return _Metadata(has_more=True, next_cursor=1337)


@pytest.fixture
def response_data():
    return [{"id": i, "name": f"row-{i}"} for i in range(10)]


@pytest.fixture
def paginated_response_no_next_page(response_data, metadata_last_page):
    return _PaginatedResponse(data=response_data, metadata=metadata_last_page)


@pytest.fixture
def paginated_response_with_next_page(response_data, metadata_next_page):
    return _PaginatedResponse(data=response_data, metadata=metadata_next_page)


@pytest.fixture
def production_runs_client(auth_token):
    return ProductionRunsApiClient(token=auth_token)


def test_timeseries_api_appends_sub_path(auth_token):
    base_url = ENLYZE_BASE_URL
    expected = str(httpx.URL(base_url).join(PRODUCTION_RUNS_API_SUB_PATH))
    client = ProductionRunsApiClient(token=auth_token, base_url=base_url)
    assert client._full_url("") == expected


@pytest.mark.parametrize(
    ("response_fixture", "expected_has_more"),
    (
        ("paginated_response_no_next_page", False),
        ("paginated_response_with_next_page", True),
    ),
)
def test_has_more(request, response_fixture, expected_has_more, production_runs_client):
    response = request.getfixturevalue(response_fixture)
    assert production_runs_client._has_more(response) == expected_has_more


def test_next_page_call_args(
    production_runs_client, endpoint, paginated_response_with_next_page
):
    params = {"some": "param"}
    kwargs = {"some": "kwarg"}
    url = endpoint
    next_url, next_params, next_kwargs = production_runs_client._next_page_call_args(
        url=url,
        params=params,
        paginated_response=paginated_response_with_next_page,
        **kwargs,
    )
    assert next_url == url
    assert next_params == {
        **params,
        "cursor": paginated_response_with_next_page.metadata.next_cursor,
    }
    assert next_kwargs == kwargs


@respx.mock
def test_timeseries_api_get_paginated_single_page(
    production_runs_client, string_model, paginated_response_no_next_page
):
    expected_data = [
        string_model.parse_obj(e) for e in paginated_response_no_next_page.data
    ]
    respx.get("").respond(json=paginated_response_no_next_page.dict())
    assert list(production_runs_client.get_paginated("", string_model)) == expected_data


@respx.mock
def test_timeseries_api_get_paginated_multi_page(
    production_runs_client,
    string_model,
    paginated_response_with_next_page,
    paginated_response_no_next_page,
):
    expected_data = [
        string_model.parse_obj(e)
        for e in [
            *paginated_response_no_next_page.data,
            *paginated_response_with_next_page.data,
        ]
    ]
    next_cursor = paginated_response_with_next_page.metadata.next_cursor
    respx.get("", params=f"cursor={next_cursor}").respond(
        200, json=paginated_response_no_next_page.dict()
    )
    respx.get("").mock(
        side_effect=lambda request: httpx.Response(
            200,
            json=paginated_response_with_next_page.dict(),
        )
    )

    assert list(production_runs_client.get_paginated("", string_model)) == expected_data
