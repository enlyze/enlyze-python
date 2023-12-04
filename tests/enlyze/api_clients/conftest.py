from unittest.mock import patch

import pytest

from enlyze.api_clients.base import ApiBaseModel


@pytest.fixture
def string_model():
    with patch(
        "enlyze.api_clients.base.ApiBaseModel.model_validate",
        side_effect=lambda o: str(o),
    ):
        yield ApiBaseModel


@pytest.fixture
def endpoint():
    return "https://my-endpoint.com"


@pytest.fixture
def base_url():
    return "http://api-client-base"
