from collections import abc
from datetime import datetime
from functools import cache, reduce
from typing import Any, Iterator, Mapping, Optional, Sequence, Union
from uuid import UUID

import enlyze.api_client.models as platform_api_models
import enlyze.models as user_models
from enlyze.api_client.client import PlatformApiClient
from enlyze.constants import (
    ENLYZE_BASE_URL,
    MAXIMUM_NUMBER_OF_VARIABLES_PER_TIMESERIES_REQUEST,
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
    pages: Iterator[platform_api_models.TimeseriesData],
) -> Optional[platform_api_models.TimeseriesData]:
    try:
        timeseries_data = next(pages)
    except StopIteration:
        return None

    if not timeseries_data.columns:
        return None

    if "time" not in timeseries_data.columns:
        raise EnlyzeError("Platform API didn't return timestamps")

    for page in pages:
        timeseries_data.extend(page)

    return timeseries_data


def validate_resampling(
    variables: Union[
        Sequence[user_models.Variable],
        Mapping[user_models.Variable, user_models.ResamplingMethod],
    ],
    resampling_interval: Optional[int],
) -> None:
    if isinstance(variables, abc.Sequence) and resampling_interval is not None:
        raise ResamplingValidationError(
            "`variables` must be a mapping {variable: ResamplingMethod}"
        )

    if resampling_interval:
        validate_resampling_interval(resampling_interval)
        for variable, resampling_method in variables.items():  # type: ignore
            validate_resampling_method_for_data_type(
                resampling_method, variable.data_type
            )


class EnlyzeClient:
    """Main entrypoint for interacting with the ENLYZE platform.

    You should instantiate it only once and use it for all requests to make the best use
    of connection pooling. This client is thread-safe.

    :param token: API token for the ENLYZE platform

    """

    def __init__(self, token: str, *, _base_url: str | None = None) -> None:
        self._platform_api_client = PlatformApiClient(
            token=token, base_url=_base_url or ENLYZE_BASE_URL
        )

    def _get_sites(self) -> Iterator[platform_api_models.Site]:
        """Get all sites from the API"""
        return self._platform_api_client.get_paginated(
            "sites", platform_api_models.Site
        )

    @cache
    def get_sites(self) -> list[user_models.Site]:
        """Retrieve all :ref:`sites <site>` of your organization.

        :returns: Sites of your organization
        :rtype: list[:class:`~enlyze.models.Site`]

        :raises: |token-error|
        :raises: |generic-error|

        """
        return [site.to_user_model() for site in self._get_sites()]

    def _get_machines(self) -> Iterator[platform_api_models.Machine]:
        """Get all machines from the API"""
        return self._platform_api_client.get_paginated(
            "machines", platform_api_models.Machine
        )

    @cache
    def get_machines(
        self, site: Optional[user_models.Site] = None
    ) -> list[user_models.Machine]:
        """Retrieve all :ref:`machines <machine>`, optionally filtered by site.

        :param site: Only get machines of this site. Gets all machines of the
            organization if None.

        :returns: Machines
        :rtype: list[:class:`~enlyze.models.Machine`]

        :raises: |token-error|
        :raises: |generic-error|

        """

        if site:
            sites_by_uuid = {site.uuid: site}
        else:
            sites_by_uuid = {site.uuid: site for site in self.get_sites()}

        machines = []
        for machine_api in self._get_machines():
            site_ = sites_by_uuid.get(machine_api.site)
            if not site_:
                continue

            machines.append(machine_api.to_user_model(site_))

        return machines

    def _get_variables(
        self, machine_uuid: UUID
    ) -> Iterator[platform_api_models.Variable]:
        """Get variables for a machine from the API."""
        return self._platform_api_client.get_paginated(
            "variables",
            platform_api_models.Variable,
            params={"machine": str(machine_uuid)},
        )

    def get_variables(
        self, machine: user_models.Machine
    ) -> Sequence[user_models.Variable]:
        """Retrieve all variables of a :ref:`machine <machine>`.

        :param machine: The machine for which to get all variables.

        :returns: Variables of ``machine``

        :raises: |token-error|
        :raises: |generic-error|

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
        variables: dict[UUID, Optional[user_models.ResamplingMethod]],
        resampling_interval: Optional[int],
    ) -> Iterator[platform_api_models.TimeseriesData]:
        request: dict[str, Any] = {
            "machine": machine_uuid,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "resampling_interval": resampling_interval,
            "variables": [
                {
                    "uuid": str(v),
                    "resampling_method": meth,
                }
                for v, meth in variables.items()
            ],
        }

        return self._platform_api_client.post_paginated(
            "timeseries",
            platform_api_models.TimeseriesData,
            json=request,
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
        validate_resampling(variables, resampling_interval)

        start, end, machine_uuid = validate_timeseries_arguments(start, end, variables)

        variable_uuids_with_resampling_method = (
            {v.uuid: meth for v, meth in variables.items()}
            if isinstance(variables, dict)
            else {v.uuid: None for v in variables}
        )

        try:
            chunks = chunk(
                list(variable_uuids_with_resampling_method.items()),
                MAXIMUM_NUMBER_OF_VARIABLES_PER_TIMESERIES_REQUEST,
            )
        except ValueError as e:
            raise EnlyzeError(FETCHING_TIMESERIES_DATA_ERROR_MSG) from e

        chunks_pages = (
            self._get_paginated_timeseries(
                machine_uuid=machine_uuid,
                start=start,
                end=end,
                variables=dict(chunk),
                resampling_interval=resampling_interval,
            )
            for chunk in chunks
        )

        timeseries_data_chunked = [
            _get_timeseries_data_from_pages(pages) for pages in chunks_pages
        ]

        if not timeseries_data_chunked or all(
            data is None for data in timeseries_data_chunked
        ):
            return None

        if any(data is None for data in timeseries_data_chunked) and any(
            data is not None for data in timeseries_data_chunked
        ):
            raise EnlyzeError(
                "The platform API didn't return data for some of the variables."
            )

        try:
            timeseries_data = reduce(lambda x, y: x.merge(y), timeseries_data_chunked)  # type: ignore # noqa
        except ValueError as e:
            raise EnlyzeError(FETCHING_TIMESERIES_DATA_ERROR_MSG) from e

        return timeseries_data.to_user_model(  # type: ignore
            start=start,
            end=end,
            variables=list(variables),
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

        :returns: Timeseries data or ``None`` if the API returned no data for the
            request

        :raises: |token-error|
        :raises: |generic-error|

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

        :returns: Timeseries data or ``None`` if the API returned no data for the
            request

        :raises: |token-error|
        :raises: |resampling-error|
        :raises: |generic-error|

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
    ) -> Iterator[platform_api_models.ProductionRun]:
        """Get production runs from the API."""

        filters = {
            "production_order": production_order,
            "product": product,
            "machine": machine,
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
        }
        params = {k: v for k, v in filters.items() if v is not None}
        return self._platform_api_client.get_paginated(
            "production-runs", platform_api_models.ProductionRun, params=params
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

        :returns: Production runs
        :rtype: :class:`~enlyze.models.ProductionRuns`

        :raises: |token-error|
        :raises: |generic-error|

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
