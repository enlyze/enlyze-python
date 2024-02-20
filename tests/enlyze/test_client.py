from datetime import datetime, timedelta
from functools import partial
from http import HTTPStatus

import httpx
import pytest
import respx
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import enlyze.api_clients.production_runs.models as production_runs_api_models
import enlyze.api_clients.timeseries.models as timeseries_api_models
import enlyze.models as user_models
from enlyze.api_clients.production_runs.client import (
    _Metadata as _ProductionRunsApiResponseMetadata,
)
from enlyze.api_clients.production_runs.client import (
    _PaginatedResponse as _PaginatedProductionRunsResponse,
)
from enlyze.api_clients.timeseries.client import (
    _PaginatedResponse as _PaginatedTimeseriesResponse,
)
from enlyze.client import EnlyzeClient
from enlyze.constants import (
    ENLYZE_BASE_URL,
    PRODUCTION_RUNS_API_SUB_PATH,
    TIMESERIES_API_SUB_PATH,
)
from enlyze.errors import EnlyzeError, ResamplingValidationError
from tests.conftest import (
    datetime_before_today_strategy,
    datetime_today_until_now_strategy,
)

MOCK_RESPONSE_HEADERS = {"Content-Type": "application/json"}

APPLIANCE_UUID = "ebef7e5a-5921-4cf3-9a52-7ff0e98e8306"
PRODUCT_CODE = "product-code"
PRODUCTION_ORDER = "production-order"
SITE_ID = 1

create_float_strategy = partial(
    st.floats, allow_nan=False, allow_infinity=False, allow_subnormal=False
)

oee_score_strategy = st.builds(
    production_runs_api_models.OEEComponent,
    score=create_float_strategy(min_value=0, max_value=1.0),
    time_loss=st.just(10),
)

quantity_strategy = st.builds(
    production_runs_api_models.Quantity,
    value=create_float_strategy(min_value=0, max_value=1.0),
)


production_runs_strategy = st.lists(
    st.builds(
        production_runs_api_models.ProductionRun,
        uuid=st.uuids(),
        start=datetime_before_today_strategy,
        end=datetime_today_until_now_strategy,
        machine=st.builds(
            production_runs_api_models.Machine, uuid=st.just(APPLIANCE_UUID)
        ),
        product=st.builds(
            production_runs_api_models.Product,
            code=st.just(PRODUCT_CODE),
        ),
        production_order=st.just(PRODUCTION_ORDER),
        availability=oee_score_strategy,
        productivity=oee_score_strategy,
        performance=oee_score_strategy,
        quality=oee_score_strategy,
        quantity_scrap=quantity_strategy,
        quantity_total=quantity_strategy,
        quantity_yield=quantity_strategy,
        average_throughput=create_float_strategy(min_value=0, max_value=1.0),
    ),
    max_size=2,
)


@pytest.fixture
def start_datetime():
    return datetime.now() - timedelta(seconds=30)


@pytest.fixture
def end_datetime():
    return datetime.now()


class PaginatedTimeseriesApiResponse(httpx.Response):
    def __init__(self, data, next=None) -> None:
        super().__init__(
            status_code=HTTPStatus.OK,
            text=_PaginatedTimeseriesResponse(data=data, next=next).model_dump_json(),
            headers=MOCK_RESPONSE_HEADERS,
        )


class PaginatedProductionRunsApiResponse(httpx.Response):
    def __init__(self, data, has_more=False, next_cursor=None) -> None:
        super().__init__(
            status_code=HTTPStatus.OK,
            text=_PaginatedProductionRunsResponse(
                data=data,
                metadata=_ProductionRunsApiResponseMetadata(
                    has_more=has_more,
                    next_cursor=next_cursor,
                ),
            ).model_dump_json(),
            headers=MOCK_RESPONSE_HEADERS,
        )


def respx_mock_with_base_url(sub_path: str = "") -> respx.MockRouter:
    base_url = httpx.URL(ENLYZE_BASE_URL).join(sub_path)
    return respx.mock(base_url=base_url)


def make_client():
    return EnlyzeClient(token="some token")


@given(
    site1=st.builds(timeseries_api_models.Site),
    site2=st.builds(timeseries_api_models.Site),
)
def test_get_sites(site1, site2):
    client = make_client()

    with respx_mock_with_base_url(TIMESERIES_API_SUB_PATH) as mock:
        mock.get("sites").mock(PaginatedTimeseriesApiResponse(data=[site1, site2]))
        sites = client.get_sites()

    assert sites == [site1.to_user_model(), site2.to_user_model()]


@given(
    site1=st.builds(timeseries_api_models.Site, id=st.just(1)),
    site2=st.builds(timeseries_api_models.Site, id=st.just(2)),
    machine1=st.builds(timeseries_api_models.Machine, site=st.just(1)),
    machine2=st.builds(timeseries_api_models.Machine, site=st.just(2)),
)
def test_get_machines(site1, site2, machine1, machine2):
    client = make_client()

    with respx_mock_with_base_url(TIMESERIES_API_SUB_PATH) as mock:
        mock.get("appliances").mock(
            PaginatedTimeseriesApiResponse(data=[machine1, machine2])
        )
        mock.get("sites").mock(PaginatedTimeseriesApiResponse(data=[site1, site2]))

        all_machines = client.get_machines()
        assert all_machines == [
            machine1.to_user_model(site1.to_user_model()),
            machine2.to_user_model(site2.to_user_model()),
        ]

        machines_site2 = client.get_machines(site2.to_user_model())
        assert machines_site2 == [
            machine2.to_user_model(site2.to_user_model()),
        ]


@given(
    machine=st.builds(timeseries_api_models.Machine),
)
def test_get_machines_site_not_found(machine):
    client = make_client()

    with respx_mock_with_base_url(TIMESERIES_API_SUB_PATH) as mock:
        mock.get("sites").mock(PaginatedTimeseriesApiResponse(data=[]))
        mock.get("appliances").mock(PaginatedTimeseriesApiResponse(data=[machine]))

        assert client.get_machines() == []


@given(
    machine=st.builds(user_models.Machine),
    var1=st.builds(timeseries_api_models.Variable),
    var2=st.builds(timeseries_api_models.Variable),
)
def test_get_variables(machine, var1, var2):
    client = make_client()

    with respx_mock_with_base_url(TIMESERIES_API_SUB_PATH) as mock:
        mock.get("variables").mock(PaginatedTimeseriesApiResponse(data=[var1, var2]))
        variables = client.get_variables(machine)

    assert variables == [
        var1.to_user_model(machine),
        var2.to_user_model(machine),
    ]


@pytest.mark.parametrize(
    "variable_strategy,timeseries_call,resampling_method_strategy",
    [
        (
            st.builds(user_models.Variable, data_type=st.just("INTEGER")),
            "without_resampling",
            st.none(),
        ),
        (
            st.builds(user_models.Variable, data_type=st.just("INTEGER")),
            "with_resampling",
            st.sampled_from(user_models.ResamplingMethod),
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
def test_get_timeseries(
    start,
    end,
    data,
    variable_strategy,
    timeseries_call,
    resampling_method_strategy,
    records,
):
    client = make_client()
    variable = data.draw(variable_strategy)

    with respx_mock_with_base_url(TIMESERIES_API_SUB_PATH) as mock:
        mock.get("timeseries", params="offset=1").mock(
            PaginatedTimeseriesApiResponse(
                data=timeseries_api_models.TimeseriesData(
                    columns=["time", str(variable.uuid)],
                    records=records[1:],
                ).model_dump()
            )
        )

        mock.get("timeseries").mock(
            side_effect=lambda request: PaginatedTimeseriesApiResponse(
                data=timeseries_api_models.TimeseriesData(
                    columns=["time", str(variable.uuid)],
                    records=records[:1],
                ).model_dump(),
                next=str(request.url.join("?offset=1")),
            )
        )
        if timeseries_call == "without_resampling":
            timeseries = client.get_timeseries(start, end, [variable])
        else:
            resampling_method = data.draw(resampling_method_strategy)
            timeseries = client.get_timeseries_with_resampling(
                start, end, {variable: resampling_method}, resampling_interval=10
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
        timeseries_api_models.TimeseriesData(columns=[], records=[]).model_dump(),
    ],
)
@pytest.mark.parametrize(
    "variable_strategy,timeseries_call,resampling_method_strategy",
    [
        (
            st.builds(user_models.Variable, data_type=st.just("INTEGER")),
            "without_resampling",
            st.none(),
        ),
        (
            st.builds(user_models.Variable, data_type=st.just("INTEGER")),
            "with_resampling",
            st.sampled_from(user_models.ResamplingMethod),
        ),
    ],
)
@given(
    data_strategy=st.data(),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_get_timeseries_returns_none_on_empty_response(
    data_strategy,
    variable_strategy,
    timeseries_call,
    resampling_method_strategy,
    data,
    start_datetime,
    end_datetime,
):
    variable = data_strategy.draw(variable_strategy)
    client = make_client()

    with respx_mock_with_base_url(TIMESERIES_API_SUB_PATH) as mock:
        mock.get("timeseries").mock(PaginatedTimeseriesApiResponse(data=data))
        if timeseries_call == "without_resampling":
            assert (
                client.get_timeseries(start_datetime, end_datetime, [variable]) is None
            )
        else:
            resampling_method = data_strategy.draw(resampling_method_strategy)
            assert (
                client.get_timeseries_with_resampling(
                    start_datetime, end_datetime, {variable: resampling_method}, 10
                )
                is None
            )


def test_get_timeseries_raises_no_variables(start_datetime, end_datetime):
    client = make_client()
    with pytest.raises(EnlyzeError, match="at least one variable"):
        client.get_timeseries(start_datetime, end_datetime, [])


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
    # we rely on variable{1,2}.machine.uuid to be different because they are random
    variable1=st.builds(user_models.Variable),
    variable2=st.builds(user_models.Variable),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_get_timeseries_raises_variables_of_different_machines(
    variable1, variable2, start_datetime, end_datetime
):
    client = make_client()

    with pytest.raises(EnlyzeError, match="for more than one machine"):
        client.get_timeseries(start_datetime, end_datetime, [variable1, variable2])


@given(
    variable=st.builds(user_models.Variable),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_get_timeseries_raises_api_returned_no_timestamps(
    variable, start_datetime, end_datetime
):
    client = make_client()

    with respx_mock_with_base_url(TIMESERIES_API_SUB_PATH) as mock:
        mock.get("timeseries").mock(
            PaginatedTimeseriesApiResponse(
                data=timeseries_api_models.TimeseriesData(
                    columns=["something but not time"],
                    records=[],
                ).model_dump()
            )
        )
        with pytest.raises(EnlyzeError, match="didn't return timestamps"):
            client.get_timeseries(start_datetime, end_datetime, [variable])


@given(
    variable=st.builds(user_models.Variable),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test__get_timeseries_raises_variables_without_resampling_method(
    start_datetime, end_datetime, variable
):
    """
    Test that `get_timeseries` will raise an `EnlyzeError` when a
    `resampling_interval` is specified but variables don't have
    resampling methods.
    """
    client = make_client()
    with pytest.raises(ResamplingValidationError):
        client._get_timeseries(start_datetime, end_datetime, [variable], 30)


@given(
    variable=st.builds(user_models.Variable),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test__get_timeseries_raises_on_chunk_value_error(
    start_datetime, end_datetime, variable, monkeypatch
):
    monkeypatch.setattr(
        "enlyze.client.MAXIMUM_NUMBER_OF_VARIABLES_PER_TIMESERIES_REQUEST", 0
    )
    client = make_client()
    with pytest.raises(EnlyzeError) as exc_info:
        client._get_timeseries(start_datetime, end_datetime, [variable])
    assert isinstance(exc_info.value.__cause__, ValueError)


@given(
    start=datetime_before_today_strategy,
    end=datetime_today_until_now_strategy,
    variable=st.builds(
        user_models.Variable,
        data_type=st.just("INTEGER"),
        machine=st.builds(timeseries_api_models.Machine),
    ),
    records=st.lists(
        st.tuples(
            datetime_today_until_now_strategy.map(datetime.isoformat),
            st.integers(),
        ),
        min_size=2,
    ),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test__get_timeseries_raises_on_merge_value_error(
    start, end, variable, records, monkeypatch
):
    client = make_client()

    def f(*args, **kwargs):
        raise ValueError

    monkeypatch.setattr("enlyze.client.reduce", f)

    with respx_mock_with_base_url(TIMESERIES_API_SUB_PATH) as mock:
        mock.get("timeseries").mock(
            PaginatedTimeseriesApiResponse(
                data=timeseries_api_models.TimeseriesData(
                    columns=["time", str(variable.uuid)],
                    records=records,
                ).model_dump()
            )
        )
        with pytest.raises(EnlyzeError):
            client._get_timeseries(start, end, [variable])


@given(
    production_order=st.just(PRODUCTION_ORDER),
    product=st.one_of(
        st.builds(user_models.Product, code=st.just(PRODUCT_CODE)),
        st.text(),
    ),
    machine=st.builds(
        timeseries_api_models.Machine,
        site=st.just(SITE_ID),
        uuid=st.just(APPLIANCE_UUID),
    ),
    site=st.builds(timeseries_api_models.Site, id=st.just(SITE_ID)),
    start=st.one_of(datetime_before_today_strategy, st.none()),
    end=st.one_of(datetime_today_until_now_strategy, st.none()),
    production_runs=production_runs_strategy,
)
def test_get_production_runs(
    production_order,
    product,
    machine,
    site,
    start,
    end,
    production_runs,
):
    client = make_client()

    site_user_model = site.to_user_model()
    machine_user_model = machine.to_user_model(site_user_model)
    machines_by_uuid = {machine.uuid: machine_user_model}

    with (
        respx_mock_with_base_url(TIMESERIES_API_SUB_PATH) as timeseries_api_mock,
        respx_mock_with_base_url(
            PRODUCTION_RUNS_API_SUB_PATH
        ) as production_runs_api_mock,
    ):
        timeseries_api_mock.get("appliances").mock(
            PaginatedTimeseriesApiResponse(data=[machine])
        )
        timeseries_api_mock.get("sites").mock(
            PaginatedTimeseriesApiResponse(data=[site])
        )
        production_runs_api_mock.get("production-runs").mock(
            PaginatedProductionRunsApiResponse(
                data=[p.model_dump() for p in production_runs]
            )
        )

        result = client.get_production_runs(
            production_order=production_order,
            product=product,
            machine=machine_user_model,
            start=start,
            end=end,
        )

        assert (
            user_models.ProductionRuns(
                [pr.to_user_model(machines_by_uuid) for pr in production_runs]
            )
            == result
        )

        df = result.to_dataframe()
        assert len(df) == len(production_runs)


@given(
    start=datetime_today_until_now_strategy,
    end=datetime_before_today_strategy,
)
def test_get_production_runs_raises_start_after_end(start, end):
    client = make_client()
    with pytest.raises(EnlyzeError, match="Start must be earlier than end"):
        client.get_production_runs(start=start, end=end)
