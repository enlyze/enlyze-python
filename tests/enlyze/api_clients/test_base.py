import string
from unittest.mock import MagicMock, call, patch

import httpx
import pytest
import respx
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from enlyze._version import VERSION
from enlyze.api_clients.base import (
    USER_AGENT_NAME_VERSION_SEPARATOR,
    ApiBaseClient,
    ApiBaseModel,
    PaginatedResponseBaseModel,
    _construct_user_agent,
)
from enlyze.constants import USER_AGENT
from enlyze.errors import EnlyzeError, InvalidTokenError


class Metadata(ApiBaseModel):
    has_more: bool
    next_cursor: int | None = None


class PaginatedResponseModel(PaginatedResponseBaseModel):
    metadata: Metadata
    data: list


def _transform_paginated_data_integers(data: list) -> list:
    return [n * n for n in data]


@pytest.fixture
def last_page_metadata():
    return Metadata(has_more=False, next_cursor=None)


@pytest.fixture
def next_page_metadata():
    return Metadata(has_more=True, next_cursor=100)


@pytest.fixture
def empty_paginated_response(last_page_metadata):
    return PaginatedResponseModel(data=[], metadata=last_page_metadata)


@pytest.fixture
def response_data_integers():
    return list(range(20))


@pytest.fixture
def paginated_response_with_next_page(response_data_integers, next_page_metadata):
    return PaginatedResponseModel(
        data=response_data_integers, metadata=next_page_metadata
    )


@pytest.fixture
def paginated_response_no_next_page(response_data_integers, last_page_metadata):
    return PaginatedResponseModel(
        data=response_data_integers, metadata=last_page_metadata
    )


@pytest.fixture
def base_client(auth_token, string_model, base_url):
    mock_has_more = MagicMock()
    mock_transform_paginated_response_data = MagicMock(side_effect=lambda e: e)
    mock_next_page_call_args = MagicMock()
    with patch.multiple(
        ApiBaseClient,
        __abstractmethods__=set(),
        _has_more=mock_has_more,
        _next_page_call_args=mock_next_page_call_args,
        _transform_paginated_response_data=mock_transform_paginated_response_data,
    ):
        client = ApiBaseClient[PaginatedResponseModel](
            token=auth_token,
            base_url=base_url,
        )
        client.PaginatedResponseModel = PaginatedResponseModel
        yield client


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
    with patch.multiple(ApiBaseClient, __abstractmethods__=set()):
        client = ApiBaseClient(token=token, base_url=base_url)

    route_is_authenticated = respx.get(
        "",
        headers__contains={"Authorization": f"Token {token}"},
    ).respond(json={})

    client.get("")
    assert route_is_authenticated.called


@respx.mock
def test_base_url(base_client, base_url):
    endpoint = "some-endpoint"

    route = respx.get(
        httpx.URL(base_url).join(endpoint),
    ).respond(json={})

    base_client.get(endpoint)
    assert route.called


@respx.mock
def test_get_raises_cannot_read(base_client):
    with pytest.raises(EnlyzeError, match="Couldn't read"):
        respx.get("").mock(side_effect=Exception("oops"))
        base_client.get("")


@respx.mock
def test_get_raises_on_error(base_client):
    with pytest.raises(EnlyzeError, match="returned error 404"):
        respx.get("").respond(404)
        base_client.get("")


@respx.mock
def test_get_raises_invalid_token_error_not_authenticated(base_client):
    with pytest.raises(InvalidTokenError):
        respx.get("").respond(403)
        base_client.get("")


@respx.mock
def test_get_raises_non_json(base_client):
    with pytest.raises(EnlyzeError, match="didn't return a valid JSON object"):
        respx.get("").respond(200, json=None)
        base_client.get("")


@respx.mock
def test_get_paginated_single_page(
    base_client, string_model, paginated_response_no_next_page
):
    endpoint = "https://irrelevant-url.com"
    params = {"params": {"param1": "value1"}}
    expected_data = [
        string_model.model_validate(e) for e in paginated_response_no_next_page.data
    ]

    mock_has_more = base_client._has_more
    mock_has_more.return_value = False
    route = respx.get(endpoint, params=params).respond(
        200, json=paginated_response_no_next_page.model_dump()
    )

    data = list(base_client.get_paginated(endpoint, ApiBaseModel, params=params))

    assert route.called
    assert route.call_count == 1
    assert expected_data == data
    mock_has_more.assert_called_once_with(paginated_response_no_next_page)


@respx.mock
def test_get_paginated_multi_page(
    base_client,
    paginated_response_with_next_page,
    paginated_response_no_next_page,
    string_model,
):
    endpoint = "https://irrelevant-url.com"
    initial_params = {"irrelevant": "values"}
    expected_data = [
        string_model.model_validate(e)
        for e in [
            *paginated_response_with_next_page.data,
            *paginated_response_no_next_page.data,
        ]
    ]

    mock_has_more = base_client._has_more
    mock_has_more.side_effect = [True, False]

    mock_next_page_call_args = base_client._next_page_call_args
    mock_next_page_call_args.return_value = (endpoint, {}, {})

    route = respx.get(endpoint)
    route.side_effect = [
        httpx.Response(200, json=paginated_response_with_next_page.model_dump()),
        httpx.Response(200, json=paginated_response_no_next_page.model_dump()),
    ]

    data = list(
        base_client.get_paginated(endpoint, ApiBaseModel, params=initial_params)
    )

    assert route.called
    assert route.call_count == 2
    assert data == expected_data
    mock_has_more.assert_has_calls(
        [
            call(paginated_response_with_next_page),
            call(paginated_response_no_next_page),
        ]
    )
    mock_next_page_call_args.assert_called_once_with(
        url=endpoint,
        params=initial_params,
        paginated_response=paginated_response_with_next_page,
    )


@pytest.mark.parametrize(
    "invalid_payload",
    [
        "not a paginated response",
        {"data": "something but not a list"},
    ],
)
@respx.mock
def test_get_paginated_raises_invalid_pagination_schema(
    base_client,
    invalid_payload,
):
    with pytest.raises(EnlyzeError, match="Paginated response expected"):
        respx.get("").respond(json=invalid_payload)
        next(
            base_client.get_paginated(
                "",
                ApiBaseModel,
            )
        )


@respx.mock
def test_get_paginated_raises_enlyze_error(
    base_client, string_model, paginated_response_no_next_page
):
    # most straightforward way to raise a pydantic.ValidationError
    # https://github.com/pydantic/pydantic/discussions/6459
    string_model.model_validate.side_effect = lambda _: Metadata()
    respx.get("").respond(200, json=paginated_response_no_next_page.model_dump())

    with pytest.raises(EnlyzeError, match="ENLYZE platform API returned an unparsable"):
        next(base_client.get_paginated("", string_model))


@respx.mock
def test_get_paginated_transform_paginated_data(
    base_client, paginated_response_no_next_page, string_model
):
    base_client._has_more.return_value = False
    base_client._transform_paginated_response_data.side_effect = (
        _transform_paginated_data_integers
    )
    expected_data = [
        string_model.model_validate(e)
        for e in _transform_paginated_data_integers(
            paginated_response_no_next_page.data
        )
    ]

    route = respx.get("").respond(
        200, json=paginated_response_no_next_page.model_dump()
    )

    data = list(base_client.get_paginated("", ApiBaseModel))

    base_client._transform_paginated_response_data.assert_called_once_with(
        paginated_response_no_next_page.data
    )

    assert route.called
    assert route.call_count == 1
    assert data == expected_data


def test_transform_paginated_data_returns_unmutated_element_by_default(
    auth_token, base_url
):
    with patch.multiple(ApiBaseClient, __abstractmethods__=set()):
        client = ApiBaseClient(token=auth_token, base_url=base_url)
        data = [1, 2, 3]
        value = client._transform_paginated_response_data(data)
        assert data == value
