import string
from unittest import mock

import httpx
import pytest
import respx
from hypothesis import given
from hypothesis import strategies as st

from enlyze.constants import ENLYZE_BASE_URL, TIMESERIES_API_SUB_PATH
from enlyze.errors import EnlyzeError, InvalidTokenError
from enlyze.timeseries_api.client import TimeseriesApiClient
from enlyze.timeseries_api.models import TimeseriesApiModel


@pytest.fixture
def string_model():
    with mock.patch(
        "enlyze.timeseries_api.models.TimeseriesApiModel.parse_obj",
        side_effect=lambda o: str(o),
    ):
        yield TimeseriesApiModel


@pytest.fixture
def client():
    return TimeseriesApiClient(token="some token")


@given(
    token=st.text(string.printable, min_size=1),
)
@respx.mock
def test_timeseries_api_token_auth(token):
    client = TimeseriesApiClient(token=token)

    route_is_authenticated = respx.get(
        "",
        headers__contains={"Authorization": f"Token {token}"},
    ).respond(json={})

    client.get("")
    assert route_is_authenticated.called


@respx.mock
def test_timeseries_api_client_base_url(client):
    endpoint = "some-endpoint"

    route = respx.get(
        httpx.URL(ENLYZE_BASE_URL).join(TIMESERIES_API_SUB_PATH).join(endpoint),
    ).respond(json={})

    client.get(endpoint)
    assert route.called


@respx.mock
def test_timeseries_api_get_raises_cannot_read(client):
    with pytest.raises(EnlyzeError, match="Couldn't read"):
        respx.get("").mock(side_effect=Exception("oops"))
        client.get("")


@respx.mock
def test_timeseries_api_get_raises_on_error(client):
    with pytest.raises(EnlyzeError, match="returned error 404"):
        respx.get("").respond(404)
        client.get("")


@respx.mock
def test_timeseries_api_get_raises_invalid_token_error_not_authenticated(client):
    with pytest.raises(InvalidTokenError):
        respx.get("").respond(403)
        client.get("")


@respx.mock
def test_timeseries_api_get_raises_non_json(client):
    with pytest.raises(EnlyzeError, match="didn't return a valid JSON object"):
        respx.get("").respond(200, json=None)
        client.get("")


@pytest.mark.parametrize(
    "invalid_payload",
    [
        "not a paginated response",
        {"data": "something but not a list"},
    ],
)
@respx.mock
def test_timeseries_api_get_paginated_raises_invalid_pagination_schema(
    client, string_model, invalid_payload
):
    with pytest.raises(EnlyzeError, match="Paginated response expected"):
        respx.get("").respond(json=invalid_payload)
        next(client.get_paginated("", string_model))


@respx.mock
def test_timeseries_api_get_paginated_raises_invalid_data_schema(client):
    respx.get("").respond(json={"data": [42, 1337]})
    with pytest.raises(EnlyzeError, match="API returned an unparsable"):
        list(client.get_paginated("", TimeseriesApiModel))


@respx.mock
def test_timeseries_api_get_paginated_data_empty(client, string_model):
    respx.get("").respond(json={"data": []})
    assert list(client.get_paginated("", string_model)) == []


@respx.mock
def test_timeseries_api_get_paginated_single_page(client, string_model):
    respx.get("").respond(json={"data": ["a", "b"]})
    assert list(client.get_paginated("", string_model)) == ["a", "b"]


@respx.mock
def test_timeseries_api_get_paginated_multi_page(client, string_model):
    respx.get("", params="offset=1").respond(json={"data": ["z"]})
    respx.get("").mock(
        side_effect=lambda request: httpx.Response(
            200,
            json={"data": ["x", "y"], "next": str(request.url.join("?offset=1"))},
        )
    )

    assert list(client.get_paginated("", string_model)) == ["x", "y", "z"]
