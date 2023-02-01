import httpx
import pytest
import respx
from hypothesis import given
from hypothesis import strategies as st

from enlyze.auth import TokenAuth
from enlyze.errors import InvalidTokenError


@respx.mock
@given(token=st.text(min_size=1))
def test_token_auth(token):
    auth = TokenAuth(token)

    my_route = respx.get("https://foo.bar/")
    response = httpx.get("https://foo.bar/", auth=auth)

    assert my_route.called
    assert response.request.headers["Authorization"] == f"Token {token}"


@pytest.mark.parametrize("invalid_token", {"", None, 0})
def test_token_auth_invalid_token(invalid_token):
    with pytest.raises(InvalidTokenError):
        TokenAuth(invalid_token)
