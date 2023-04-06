from datetime import datetime, timedelta

import httpx
import pytest
import respx
from hypothesis import given
from hypothesis import strategies as st

import enlyze.models as user_models
import enlyze.timeseries_api.models as api_models
from enlyze.client import EnlyzeClient
from enlyze.constants import ENLYZE_BASE_URL, TIMESERIES_API_SUB_PATH
from enlyze.errors import EnlyzeError
from enlyze.timeseries_api.client import _PaginatedResponse
from tests.conftest import (
    datetime_before_today_strategy,
    datetime_today_until_now_strategy,
)


class PaginatedResponse(httpx.Response):
    def __init__(self, data, next=None) -> None:
        super().__init__(
            status_code=200,
            text=_PaginatedResponse(data=data, next=next).json(),
            headers={"Content-Type": "application/json"},
        )


def respx_mock_with_base_url() -> respx.MockRouter:
    return respx.mock(base_url=f"{ENLYZE_BASE_URL}/{TIMESERIES_API_SUB_PATH}")


def make_client():
    return EnlyzeClient(token="some token")


@given(
    site1=st.builds(api_models.Site),
    site2=st.builds(api_models.Site),
)
def test_get_sites(site1, site2):
    client = make_client()

    with respx_mock_with_base_url() as mock:
        mock.get("sites").mock(PaginatedResponse(data=[site1, site2]))
        sites = client.get_sites()

    assert sites == [site1.to_user_model(), site2.to_user_model()]


@given(
    site1=st.builds(api_models.Site, id=st.just(1)),
    site2=st.builds(api_models.Site, id=st.just(2)),
    appliance1=st.builds(api_models.Appliance, site=st.just(1)),
    appliance2=st.builds(api_models.Appliance, site=st.just(2)),
)
def test_get_appliances(site1, site2, appliance1, appliance2):
    client = make_client()

    with respx_mock_with_base_url() as mock:
        mock.get("appliances").mock(PaginatedResponse(data=[appliance1, appliance2]))
        mock.get("sites").mock(PaginatedResponse(data=[site1, site2]))

        all_appliances = client.get_appliances()
        assert all_appliances == [
            appliance1.to_user_model(site1.to_user_model()),
            appliance2.to_user_model(site2.to_user_model()),
        ]

        appliances_site2 = client.get_appliances(site2.to_user_model())
        assert appliances_site2 == [
            appliance2.to_user_model(site2.to_user_model()),
        ]


@given(
    appliance=st.builds(api_models.Appliance),
)
def test_get_appliances_site_not_found(appliance):
    client = make_client()

    with respx_mock_with_base_url() as mock:
        mock.get("sites").mock(PaginatedResponse(data=[]))
        mock.get("appliances").mock(PaginatedResponse(data=[appliance]))

        assert client.get_appliances() == []


@given(
    appliance=st.builds(user_models.Appliance),
    var1=st.builds(api_models.Variable),
    var2=st.builds(api_models.Variable),
)
def test_get_variables(appliance, var1, var2):
    client = make_client()

    with respx_mock_with_base_url() as mock:
        mock.get("variables").mock(PaginatedResponse(data=[var1, var2]))
        variables = client.get_variables(appliance)

    assert variables == [
        var1.to_user_model(appliance),
        var2.to_user_model(appliance),
    ]


@pytest.mark.parametrize(
    "variable_strategy,timeseries_call",
    [
        (
            st.builds(user_models.Variable, data_type=st.just("INTEGER")),
            "without_resampling",
        ),
        (
            st.builds(
                user_models.VariableWithResamplingMethod, data_type=st.just("INTEGER")
            ),
            "with_resampling",
        ),
    ],
)
@given(
    start=datetime_before_today_strategy,
    end=datetime_today_until_now_strategy,
    data=st.data(),
    records=st.lists(
        st.tuples(
            datetime_today_until_now_strategy.map(datetime.isoformat),
            st.integers(),
        ),
        min_size=2,
    ),
)
def test_get_timeseries(start, end, data, variable_strategy, timeseries_call, records):
    client = make_client()
    variable = data.draw(variable_strategy)

    with respx_mock_with_base_url() as mock:
        mock.get("timeseries", params="offset=1").mock(
            PaginatedResponse(
                data=api_models.TimeseriesData(
                    columns=["time", str(variable.uuid)],
                    records=records[1:],
                ).dict()
            )
        )

        mock.get("timeseries").mock(
            side_effect=lambda request: PaginatedResponse(
                data=api_models.TimeseriesData(
                    columns=["time", str(variable.uuid)],
                    records=records[:1],
                ).dict(),
                next=str(request.url.join("?offset=1")),
            )
        )
        if timeseries_call == "without_resampling":
            timeseries = client.get_timeseries(start, end, [variable])
        else:
            timeseries = client.get_timeseries_with_resampling(
                start, end, [variable], resampling_interval=10
            )
        assert len(timeseries) == len(records)

    assert f"{len(records)} records" in str(timeseries)

    column_name = variable.display_name or str(variable.uuid)

    df = timeseries.to_dataframe(use_display_names=True)
    assert len(df) == len(records)
    assert df.index.name == "time"
    assert isinstance(df.index[0], datetime)
    assert df[column_name].iloc[0] == records[0][1]

    dicts = list(timeseries.to_dicts(use_display_names=True))
    assert len(dicts) == len(records)
    assert "time" in dicts[0]
    assert isinstance(dicts[0]["time"], datetime)
    assert dicts[0][column_name] == records[0][1]


@pytest.mark.parametrize(
    "data",
    [
        {},
        api_models.TimeseriesData(columns=[], records=[]).dict(),
    ],
)
@pytest.mark.parametrize(
    "variable_strategy,timeseries_call",
    [
        (
            st.builds(user_models.Variable, data_type=st.just("INTEGER")),
            "without_resampling",
        ),
        (
            st.builds(
                user_models.VariableWithResamplingMethod, data_type=st.just("INTEGER")
            ),
            "with_resampling",
        ),
    ],
)
@given(
    data_strategy=st.data(),
)
def test_get_timeseries_returns_none_on_empty_response(
    data_strategy, variable_strategy, timeseries_call, data
):
    variable = data_strategy.draw(variable_strategy)
    client = make_client()

    with respx_mock_with_base_url() as mock:
        mock.get("timeseries").mock(PaginatedResponse(data=data))
        if timeseries_call == "without_resampling":
            assert (
                client.get_timeseries(datetime.now(), datetime.now(), [variable])
                is None
            )
        else:
            assert (
                client.get_timeseries_with_resampling(
                    datetime.now(), datetime.now(), [variable], 10
                )
                is None
            )


def test_get_timeseries_raises_no_variables():
    client = make_client()

    with pytest.raises(EnlyzeError, match="at least one variable"):
        client.get_timeseries(datetime.now(), datetime.now(), [])


@given(
    variable=st.builds(user_models.Variable),
)
def test_get_timeseries_raises_invalid_time_bounds(variable):
    client = make_client()

    with pytest.raises(EnlyzeError, match="Start must be earlier than end"):
        client.get_timeseries(
            datetime.now() + timedelta(days=1), datetime.now(), [variable]
        )


@given(
    # we rely on variable{1,2}.appliance.uuid to be different because they are random
    variable1=st.builds(user_models.Variable),
    variable2=st.builds(user_models.Variable),
)
def test_get_timeseries_raises_variables_of_different_appliances(variable1, variable2):
    client = make_client()

    with pytest.raises(EnlyzeError, match="for more than one appliance"):
        client.get_timeseries(datetime.now(), datetime.now(), [variable1, variable2])


@given(
    variable=st.builds(user_models.Variable),
)
def test_get_timeseries_raises_api_returned_no_timestamps(variable):
    client = make_client()

    with respx_mock_with_base_url() as mock:
        mock.get("timeseries").mock(
            PaginatedResponse(
                data=api_models.TimeseriesData(
                    columns=["something but not time"],
                    records=[],
                ).dict()
            )
        )

        with pytest.raises(EnlyzeError, match="didn't return timestamps"):
            client.get_timeseries(datetime.now(), datetime.now(), [variable])
