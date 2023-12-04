import httpx
import pytest
import respx

from enlyze.api_clients.timeseries.client import TimeseriesApiClient, _PaginatedResponse
from enlyze.constants import TIMESERIES_API_SUB_PATH


@pytest.fixture
def response_data_list() -> list:
    return [1, 2, 3]


@pytest.fixture
def response_data_dict() -> dict:
    return {"some": "dictionary"}


@pytest.fixture
def transformed_data_dict(response_data_dict) -> list[dict]:
    return [response_data_dict]


@pytest.fixture
def paginated_response_no_next_page():
    return _PaginatedResponse(data=[], next=None)


@pytest.fixture
def paginated_response_with_next_page(endpoint):
    return _PaginatedResponse(
        data=[],
        next=f"{endpoint}?offset=1337",
    )


@pytest.fixture
def timeseries_client(auth_token, base_url):
    return TimeseriesApiClient(token=auth_token, base_url=base_url)


def test_timeseries_api_appends_sub_path(auth_token, base_url):
    expected = str(httpx.URL(base_url).join(TIMESERIES_API_SUB_PATH))
    client = TimeseriesApiClient(token=auth_token, base_url=base_url)
    assert client._full_url("") == expected


@pytest.mark.parametrize(
    ("response_fixture", "expected_has_more"),
    (
        ("paginated_response_no_next_page", False),
        ("paginated_response_with_next_page", True),
    ),
)
def test_has_more(request, response_fixture, expected_has_more, timeseries_client):
    response = request.getfixturevalue(response_fixture)
    assert timeseries_client._has_more(response) == expected_has_more


@pytest.mark.parametrize(
    ("data_fixture", "expected_fixture"),
    (
        ("response_data_list", "response_data_list"),
        ("response_data_dict", "transformed_data_dict"),
    ),
)
def test_get_paginated_transform_paginated_data(
    request, timeseries_client, data_fixture, expected_fixture
):
    data = request.getfixturevalue(data_fixture)
    expected = request.getfixturevalue(expected_fixture)
    assert timeseries_client._transform_paginated_response_data(data) == expected


def test_next_page_call_args(
    timeseries_client, endpoint, paginated_response_with_next_page
):
    params = {"some": "param"}
    kwargs = {"some": "kwarg"}
    url = endpoint
    next_url, next_params, next_kwargs = timeseries_client._next_page_call_args(
        url=url,
        params=params,
        paginated_response=paginated_response_with_next_page,
        **kwargs,
    )
    assert next_url == str(paginated_response_with_next_page.next)
    assert next_params == params
    assert next_kwargs == kwargs


@respx.mock
def test_timeseries_api_get_paginated_single_page(timeseries_client, string_model):
    respx.get("").respond(json={"data": ["a", "b"], "next": None})
    assert list(timeseries_client.get_paginated("", string_model)) == ["a", "b"]


@respx.mock
def test_timeseries_api_get_paginated_multi_page(timeseries_client, string_model):
    respx.get("", params="offset=1").respond(json={"data": ["z"], "next": None})
    respx.get("").mock(
        side_effect=lambda request: httpx.Response(
            200,
            json={"data": ["x", "y"], "next": str(request.url.join("?offset=1"))},
        )
    )

    assert list(timeseries_client.get_paginated("", string_model)) == ["x", "y", "z"]
