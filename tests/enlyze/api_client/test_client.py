import string
from unittest.mock import patch

import httpx
import pytest
import respx
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from enlyze._version import VERSION
from enlyze.api_client.client import (
    USER_AGENT_NAME_VERSION_SEPARATOR,
    PlatformApiClient,
    PlatformApiModel,
    _construct_user_agent,
    _Metadata,
    _PaginatedResponse,
)
from enlyze.constants import USER_AGENT
from enlyze.errors import EnlyzeError, InvalidTokenError


def _paginated_responses_to_expected_data(
    model: PlatformApiModel, paginated_responses: list[_PaginatedResponse]
) -> list:
    expected = []
    for r in paginated_responses:
        data = r.data if isinstance(r.data, list) else [r.data]
        validated = [model.model_validate(e) for e in data]
        expected.extend(validated)
    return expected


@pytest.fixture
def string_model():
    with patch(
        "enlyze.api_client.models.PlatformApiModel.model_validate",
        side_effect=lambda o: str(o),
    ):
        yield PlatformApiModel


@pytest.fixture
def base_url():
    return "http://api-client-base"


@pytest.fixture
def api_client(auth_token, base_url):
    return PlatformApiClient(token=auth_token, base_url=base_url)


@pytest.fixture
def api_client_base_url(api_client):
    return api_client._client.base_url


@pytest.fixture
def last_page_metadata():
    return _Metadata(next_cursor=None)


@pytest.fixture
def next_page_metadata():
    return _Metadata(next_cursor="100")


@pytest.fixture
def response_data_dict() -> dict:
    return {"some": "dictionary"}


@pytest.fixture
def response_data_list(response_data_dict) -> list:
    return [response_data_dict]


@pytest.fixture
def empty_paginated_response(last_page_metadata):
    return _PaginatedResponse(data=[], metadata=last_page_metadata)


@pytest.fixture
def paginated_response_list_no_next_page(response_data_list, last_page_metadata):
    return _PaginatedResponse(data=response_data_list, metadata=last_page_metadata)


@pytest.fixture
def paginated_response_dict_no_next_page(response_data_dict, last_page_metadata):
    return _PaginatedResponse(data=response_data_dict, metadata=last_page_metadata)


@pytest.fixture
def paginated_response_list_with_next_page(response_data_list, next_page_metadata):
    return _PaginatedResponse(data=response_data_list, metadata=next_page_metadata)


@pytest.fixture
def paginated_response_dict_with_next_page(response_data_dict, next_page_metadata):
    return _PaginatedResponse(data=response_data_dict, metadata=next_page_metadata)


@pytest.fixture
def custom_user_agent():
    return "custom-user-agent"


@pytest.fixture
def custom_user_agent_version():
    return "3.4.5"


class TestConstructUserAgent:
    def test__construct_user_agent_with_defaults(self):
        ua, version = _construct_user_agent().split(USER_AGENT_NAME_VERSION_SEPARATOR)
        assert ua == USER_AGENT
        assert version == VERSION

    def test__construct_user_agent_custom_agent(self, custom_user_agent):
        ua, version = _construct_user_agent(user_agent=custom_user_agent).split(
            USER_AGENT_NAME_VERSION_SEPARATOR
        )
        assert ua == custom_user_agent
        assert version == VERSION

    def test__construct_user_agent_custom_version(self, custom_user_agent_version):
        ua, version = _construct_user_agent(version=custom_user_agent_version).split(
            USER_AGENT_NAME_VERSION_SEPARATOR
        )
        assert ua == USER_AGENT
        assert version == custom_user_agent_version

    def test__construct_user_agent_custom_agent_and_version(
        self, custom_user_agent, custom_user_agent_version
    ):
        ua, version = _construct_user_agent(
            user_agent=custom_user_agent, version=custom_user_agent_version
        ).split(USER_AGENT_NAME_VERSION_SEPARATOR)
        assert ua == custom_user_agent
        assert version == custom_user_agent_version


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    token=st.text(string.printable, min_size=1),
)
@respx.mock
def test_token_auth(token, base_url):
    route_is_authenticated = respx.get(
        "",
        headers__contains={"Authorization": f"Bearer {token}"},
    ).respond(json={})

    api_client = PlatformApiClient(base_url=base_url, token=token)
    api_client.get("")
    assert route_is_authenticated.called


@respx.mock
def test_base_url(api_client, api_client_base_url):
    endpoint = "some-endpoint"

    route = respx.get(
        httpx.URL(api_client_base_url).join(endpoint),
    ).respond(json={})

    api_client.get(endpoint)
    assert route.called


@respx.mock
def test_get_raises_cannot_read(api_client):
    with pytest.raises(EnlyzeError, match="Couldn't read"):
        respx.get("").mock(side_effect=Exception("oops"))
        api_client.get("")


@respx.mock
def test_get_raises_on_error(api_client):
    with pytest.raises(EnlyzeError, match="returned error 404"):
        respx.get("").respond(404)
        api_client.get("")


@respx.mock
def test_get_raises_invalid_token_error_not_authenticated(api_client):
    with pytest.raises(InvalidTokenError):
        respx.get("").respond(403)
        api_client.get("")


@respx.mock
def test_get_raises_non_json(api_client):
    with pytest.raises(EnlyzeError, match="didn't return a valid JSON object"):
        respx.get("").respond(200, json=None)
        api_client.get("")


@pytest.mark.parametrize(
    "invalid_payload",
    [
        "not a paginated response",
        {"data": "something but not a list"},
    ],
)
@respx.mock
def test_get_paginated_raises_invalid_pagination_schema(
    api_client, string_model, invalid_payload
):
    with pytest.raises(EnlyzeError, match="Paginated response expected"):
        respx.get("").respond(json=invalid_payload)
        next(api_client.get_paginated("", string_model))


@pytest.mark.parametrize(
    "paginated_response_no_next_page_fixture",
    ["paginated_response_list_no_next_page", "paginated_response_dict_no_next_page"],
)
@respx.mock
def test_get_paginated_single_page(
    api_client,
    string_model,
    paginated_response_no_next_page_fixture,
    request,
):
    paginated_response_no_next_page = request.getfixturevalue(
        paginated_response_no_next_page_fixture
    )
    params = {"params": {"param1": "value1"}}
    expected_data = _paginated_responses_to_expected_data(
        string_model, [paginated_response_no_next_page]
    )

    route = respx.get("", params=params).respond(
        200, json=paginated_response_no_next_page.model_dump()
    )

    data = list(api_client.get_paginated("", string_model, params=params))

    assert route.called
    assert route.call_count == 1
    assert expected_data == data


@pytest.mark.parametrize(
    "paginated_response_with_next_page_fixture,paginated_response_no_next_page_fixture",
    [
        [
            "paginated_response_dict_with_next_page",
            "paginated_response_dict_no_next_page",
        ],
        [
            "paginated_response_list_with_next_page",
            "paginated_response_list_no_next_page",
        ],
    ],
)
@respx.mock
def test_get_paginated_multi_page(
    api_client,
    paginated_response_with_next_page_fixture,
    paginated_response_no_next_page_fixture,
    string_model,
    request,
):
    initial_params = {"irrelevant": "values"}
    responses = [
        request.getfixturevalue(paginated_response_with_next_page_fixture),
        request.getfixturevalue(paginated_response_no_next_page_fixture),
    ]

    expected_data = _paginated_responses_to_expected_data(string_model, responses)

    route = respx.get("", params=initial_params)
    route.side_effect = [httpx.Response(200, json=r.model_dump()) for r in responses]

    data = list(api_client.get_paginated("", PlatformApiModel, params=initial_params))

    assert route.called
    assert route.call_count == 2
    assert data == expected_data
