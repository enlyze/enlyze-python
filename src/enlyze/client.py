from collections import abc
from datetime import datetime
from functools import cache, reduce
from typing import Any, Iterator, Mapping, Optional, Sequence, Tuple, Union
from uuid import UUID

import enlyze.api_clients.timeseries.models as timeseries_api_models
import enlyze.models as user_models
from enlyze.api_clients.production_runs.client import ProductionRunsApiClient
from enlyze.api_clients.production_runs.models import ProductionRun
from enlyze.api_clients.timeseries.client import TimeseriesApiClient
from enlyze.constants import (
    ENLYZE_BASE_URL,
    MAXIMUM_NUMBER_OF_VARIABLES_PER_TIMESERIES_REQUEST,
    VARIABLE_UUID_AND_RESAMPLING_METHOD_SEPARATOR,
)
from enlyze.errors import EnlyzeError, ResamplingValidationError
from enlyze.iterable_tools import chunk
from enlyze.validators import (
    validate_datetime,
    validate_resampling_interval,
    validate_resampling_method_for_data_type,
    validate_start_and_end,
    validate_timeseries_arguments,
)

FETCHING_TIMESERIES_DATA_ERROR_MSG = "Error occurred when fetching timeseries data."


def _get_timeseries_data_from_pages(
    pages: Iterator[timeseries_api_models.TimeseriesData],
) -> Optional[timeseries_api_models.TimeseriesData]:
    try:
        timeseries_data = next(pages)
    except StopIteration:
        return None

    if not timeseries_data.columns:
        return None

    if "time" not in timeseries_data.columns:
        raise EnlyzeError("Timeseries API didn't return timestamps")

    for page in pages:
        timeseries_data.extend(page)

    return timeseries_data


def _get_variables_sequence_and_query_parameter_list(
    variables: Union[
        Sequence[user_models.Variable],
        Mapping[user_models.Variable, user_models.ResamplingMethod],
    ],
    resampling_interval: Optional[int],
) -> Tuple[Sequence[user_models.Variable], Sequence[str]]:
    if isinstance(variables, abc.Sequence) and resampling_interval is not None:
        raise ResamplingValidationError(
            "`variables` must be a mapping {variable: ResamplingMethod}"
        )

    if resampling_interval:
        validate_resampling_interval(resampling_interval)
        variables_sequence = []
        variables_query_parameter_list = []
        for variable, resampling_method in variables.items():  # type: ignore
            variables_sequence.append(variable)
            variables_query_parameter_list.append(
                f"{variable.uuid}"
                f"{VARIABLE_UUID_AND_RESAMPLING_METHOD_SEPARATOR}"
                f"{resampling_method.value}"
            )

            validate_resampling_method_for_data_type(
                resampling_method, variable.data_type
            )
        return variables_sequence, variables_query_parameter_list

    return variables, [str(v.uuid) for v in variables]  # type: ignore


class EnlyzeClient:
    """Main entrypoint for interacting with the ENLYZE platform.

    You should instantiate it only once and use it for all requests to make the best use
    of connection pooling. This client is thread-safe.

    :param token: API token for the ENLYZE platform

    """

    def __init__(self, token: str, *, _base_url: str | None = None) -> None:
        self._timeseries_api_client = TimeseriesApiClient(
            token=token,
            base_url=_base_url or ENLYZE_BASE_URL,
        )
        self._production_runs_api_client = ProductionRunsApiClient(
            token=token,
            base_url=_base_url or ENLYZE_BASE_URL,
        )

    def _get_sites(self) -> Iterator[timeseries_api_models.Site]:
        """Get all sites from the API"""
        return self._timeseries_api_client.get_paginated(
            "sites", timeseries_api_models.Site
        )

    @cache
    def get_sites(self) -> list[user_models.Site]:
        """Retrieve all :ref:`sites <site>` of your organization.

        :raises: |token-error|

        :raises: |generic-error|

        :returns: Sites of your organization
        :rtype: list[:class:`~enlyze.models.Site`]

        """
        return [site.to_user_model() for site in self._get_sites()]

    def _get_machines(self) -> Iterator[timeseries_api_models.Machine]:
        """Get all machines from the API"""
        return self._timeseries_api_client.get_paginated(
            "appliances", timeseries_api_models.Machine
        )

    @cache
    def get_machines(
        self, site: Optional[user_models.Site] = None
    ) -> list[user_models.Machine]:
        """Retrieve all :ref:`machines <machine>`, optionally filtered by site.

        :param site: Only get machines of this site. Gets all machines of the
            organization if None.

        :raises: |token-error|

        :raises: |generic-error|

        :returns: Machines
        :rtype: list[:class:`~enlyze.models.Machine`]

        """

        if site:
            sites_by_id = {site._id: site}
        else:
            sites_by_id = {site._id: site for site in self.get_sites()}

        machines = []
        for machine_api in self._get_machines():
            site_ = sites_by_id.get(machine_api.site)
            if not site_:
                continue

            machines.append(machine_api.to_user_model(site_))

        return machines

    def _get_variables(
        self, machine_uuid: UUID
    ) -> Iterator[timeseries_api_models.Variable]:
        """Get variables for a machine from the API."""
        return self._timeseries_api_client.get_paginated(
            "variables",
            timeseries_api_models.Variable,
            params={"appliance": str(machine_uuid)},
        )

    def get_variables(
        self, machine: user_models.Machine
    ) -> Sequence[user_models.Variable]:
        """Retrieve all variables of a :ref:`machine <machine>`.

        :param machine: The machine for which to get all variables.

        :raises: |token-error|

        :raises: |generic-error|

        :returns: Variables of ``machine``

        """
        return [
            variable.to_user_model(machine)
            for variable in self._get_variables(machine.uuid)
        ]

    def _get_paginated_timeseries(
        self,
        *,
        machine_uuid: str,
        start: datetime,
        end: datetime,
        variables: Sequence[str],
        resampling_interval: Optional[int],
    ) -> Iterator[timeseries_api_models.TimeseriesData]:
        params: dict[str, Any] = {
            "appliance": machine_uuid,
            "start_datetime": start.isoformat(),
            "end_datetime": end.isoformat(),
            "variables": ",".join(variables),
        }

        if resampling_interval:
            params["resampling_interval"] = resampling_interval

        return self._timeseries_api_client.get_paginated(
            "timeseries", timeseries_api_models.TimeseriesData, params=params
        )

    def _get_timeseries(
        self,
        start: datetime,
        end: datetime,
        variables: Union[
            Sequence[user_models.Variable],
            Mapping[user_models.Variable, user_models.ResamplingMethod],
        ],
        resampling_interval: Optional[int] = None,
    ) -> Optional[user_models.TimeseriesData]:
        variables_sequence, variables_query_parameter_list = (
            _get_variables_sequence_and_query_parameter_list(
                variables, resampling_interval
            )
        )

        start, end, machine_uuid = validate_timeseries_arguments(
            start, end, variables_sequence
        )

        try:
            chunks = chunk(
                variables_query_parameter_list,
                MAXIMUM_NUMBER_OF_VARIABLES_PER_TIMESERIES_REQUEST,
            )
        except ValueError as e:
            raise EnlyzeError(FETCHING_TIMESERIES_DATA_ERROR_MSG) from e

        chunks_pages = (
            self._get_paginated_timeseries(
                machine_uuid=machine_uuid,
                start=start,
                end=end,
                variables=chunk,
                resampling_interval=resampling_interval,
            )
            for chunk in chunks
        )

        timeseries_data_chunked = [
            _get_timeseries_data_from_pages(pages) for pages in chunks_pages
        ]
        if not timeseries_data_chunked or None in timeseries_data_chunked:
            return None

        try:
            timeseries_data = reduce(lambda x, y: x.merge(y), timeseries_data_chunked)  # type: ignore # noqa
        except ValueError as e:
            raise EnlyzeError(FETCHING_TIMESERIES_DATA_ERROR_MSG) from e

        return timeseries_data.to_user_model(  # type: ignore
            start=start,
            end=end,
            variables=variables_sequence,
        )

    def get_timeseries(
        self,
        start: datetime,
        end: datetime,
        variables: Sequence[user_models.Variable],
    ) -> Optional[user_models.TimeseriesData]:
        """Get timeseries data of :ref:`variables <variable>` for a given time frame.

        Timeseries data for multiple variables can be requested at once. However, all
        variables must belong to the same machine.

        You should always pass :ref:`timezone-aware datetime
        <python:datetime-naive-aware>` objects to this method! If you don't, naive
        datetime objects will be assumed to be expressed in the local timezone of the
        system where the code is run.

        :param start: Start of the time frame for which to fetch timeseries data. Must
            not be before ``end``.
        :param end: End of the time frame for which to fetch timeseries data.
        :param variables: The variables for which to fetch timeseries data.

        :raises: |token-error|

        :raises: |generic-error|

        :returns: Timeseries data or ``None`` if the API returned no data for the
            request

        """

        return self._get_timeseries(start, end, variables)

    def get_timeseries_with_resampling(
        self,
        start: datetime,
        end: datetime,
        variables: Mapping[user_models.Variable, user_models.ResamplingMethod],
        resampling_interval: int,
    ) -> Optional[user_models.TimeseriesData]:
        """Get resampled timeseries data of :ref:`variables <variable>` for a given time frame.

        Timeseries data for multiple variables can be requested at once. However, all
        variables must belong to the same machine.

        You should always pass :ref:`timezone-aware datetime
        <python:datetime-naive-aware>` objects to this method! If you don't, naive
        datetime objects will be assumed to be expressed in the local timezone of the
        system where the code is run.

        :param start: Start of the time frame for which to fetch timeseries data. Must
            not be before ``end``.
        :param end: End of the time frame for which to fetch timeseries data.
        :param variables: The variables for which to fetch timeseries data along with a
            :class:`~enlyze.models.ResamplingMethod` for each variable. Resampling isn't
            supported for variables whose :class:`~enlyze.models.VariableDataType` is
            one of the `ARRAY` data types.
        :param resampling_interval: The interval in seconds to resample timeseries data
            with. Must be greater than or equal
            :const:`~enlyze.constants.MINIMUM_RESAMPLING_INTERVAL`.

        :raises: |token-error|

        :raises: |resampling-error|

        :raises: |generic-error|

        :returns: Timeseries data or ``None`` if the API returned no data for the
            request

        """  # noqa: E501
        return self._get_timeseries(start, end, variables, resampling_interval)

    def _get_production_runs(
        self,
        *,
        production_order: Optional[str] = None,
        product: Optional[str] = None,
        machine: Optional[UUID] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> Iterator[ProductionRun]:
        """Get production runs from the API."""

        filters = {
            "production_order": production_order,
            "product": product,
            "appliance": machine,
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
        }
        params = {k: v for k, v in filters.items() if v is not None}
        return self._production_runs_api_client.get_paginated(
            "production-runs", ProductionRun, params=params
        )

    def get_production_runs(
        self,
        *,
        production_order: Optional[str] = None,
        product: Optional[user_models.Product | str] = None,
        machine: Optional[user_models.Machine] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> user_models.ProductionRuns:
        """Retrieve optionally filtered list of :ref:`production runs <production_run>`.

        :param machine: The machine for which to get all production runs.
        :param product: Filter production runs by product.
        :param production_order: Filter production runs by production order.

        :raises: |token-error|

        :raises: |generic-error|

        :returns: Production runs
        :rtype: :class:`~enlyze.models.ProductionRuns`

        """
        if start and end:
            start, end = validate_start_and_end(start, end)
        elif start:
            start = validate_datetime(start)
        elif end:
            end = validate_datetime(end)

        product_filter = (
            product.code if isinstance(product, user_models.Product) else product
        )
        machines_by_uuid = {a.uuid: a for a in self.get_machines()}
        return user_models.ProductionRuns(
            [
                production_run.to_user_model(machines_by_uuid)
                for production_run in self._get_production_runs(
                    machine=machine.uuid if machine else None,
                    production_order=production_order,
                    product=product_filter,
                    start=start,
                    end=end,
                )
            ]
        )
