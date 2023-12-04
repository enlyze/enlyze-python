from datetime import datetime
from functools import cache
from typing import Iterator, Mapping, Optional, Sequence
from uuid import UUID

import enlyze.api_clients.timeseries.models as timeseries_api_models
import enlyze.models as user_models
from enlyze.api_clients.production_runs.client import ProductionRunsApiClient
from enlyze.api_clients.production_runs.models import ProductionRun
from enlyze.api_clients.timeseries.client import TimeseriesApiClient
from enlyze.constants import (
    ENLYZE_BASE_URL,
    VARIABLE_UUID_AND_RESAMPLING_METHOD_SEPARATOR,
)
from enlyze.errors import EnlyzeError
from enlyze.validators import (
    validate_datetime,
    validate_resampling_interval,
    validate_resampling_method_for_data_type,
    validate_start_and_end,
    validate_timeseries_arguments,
)


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

    def _get_appliances(self) -> Iterator[timeseries_api_models.Appliance]:
        """Get all appliances from the API"""
        return self._timeseries_api_client.get_paginated(
            "appliances", timeseries_api_models.Appliance
        )

    @cache
    def get_appliances(
        self, site: Optional[user_models.Site] = None
    ) -> list[user_models.Appliance]:
        """Retrieve all :ref:`appliances <appliance>`, optionally filtered by site.

        :param site: Only get appliances of this site. Gets all appliances of the
            organization if None.
        :type site: :class:`~enlyze.models.Site` or None

        :raises: |token-error|

        :raises: |generic-error|

        :returns: Appliances
        :rtype: list[:class:`~enlyze.models.Appliance`]

        """

        if site:
            sites_by_id = {site._id: site}
        else:
            sites_by_id = {site._id: site for site in self.get_sites()}

        appliances = []
        for appliance_api in self._get_appliances():
            site_ = sites_by_id.get(appliance_api.site)
            if not site_:
                continue

            appliances.append(appliance_api.to_user_model(site_))

        return appliances

    def _get_variables(
        self, appliance_uuid: UUID
    ) -> Iterator[timeseries_api_models.Variable]:
        """Get variables for an appliance from the API."""
        return self._timeseries_api_client.get_paginated(
            "variables",
            timeseries_api_models.Variable,
            params={"appliance": str(appliance_uuid)},
        )

    def get_variables(
        self, appliance: user_models.Appliance
    ) -> Sequence[user_models.Variable]:
        """Retrieve all variables of an :ref:`appliance <appliance>`.

        :param appliance: The appliance for which to get all variables.
        :type appliance: :class:`~enlyze.models.Appliance`

        :raises: |token-error|

        :raises: |generic-error|

        :returns: Variables of ``appliance``

        """
        return [
            variable.to_user_model(appliance)
            for variable in self._get_variables(appliance.uuid)
        ]

    def get_timeseries(
        self,
        start: datetime,
        end: datetime,
        variables: Sequence[user_models.Variable],
    ) -> Optional[user_models.TimeseriesData]:
        """Get timeseries data of :ref:`variables <variable>` for a given time frame.

        Timeseries data for multiple variables can be requested at once. However, all
        variables must belong to the same appliance.

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

        start, end, appliance_uuid = validate_timeseries_arguments(
            start, end, variables
        )

        pages = self._timeseries_api_client.get_paginated(
            "timeseries",
            timeseries_api_models.TimeseriesData,
            params={
                "appliance": appliance_uuid,
                "start_datetime": start.isoformat(),
                "end_datetime": end.isoformat(),
                "variables": ",".join(str(v.uuid) for v in variables),
            },
        )

        timeseries_data = _get_timeseries_data_from_pages(pages)
        if timeseries_data is None:
            return None

        return timeseries_data.to_user_model(
            start=start,
            end=end,
            variables=variables,
        )

    def get_timeseries_with_resampling(
        self,
        start: datetime,
        end: datetime,
        variables: Mapping[user_models.Variable, user_models.ResamplingMethod],
        resampling_interval: int,
    ) -> Optional[user_models.TimeseriesData]:
        """Get resampled timeseries data of :ref:`variables <variable>` for a given time frame.

        Timeseries data for multiple variables can be requested at once. However, all
        variables must belong to the same appliance.

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
        variables_sequence = []
        variables_query_parameter_list = []
        for variable, resampling_method in variables.items():
            variables_sequence.append(variable)
            variables_query_parameter_list.append(
                f"{variable.uuid}"
                f"{VARIABLE_UUID_AND_RESAMPLING_METHOD_SEPARATOR}"
                f"{resampling_method.value}"
            )

            validate_resampling_method_for_data_type(
                resampling_method, variable.data_type
            )

        start, end, appliance_uuid = validate_timeseries_arguments(
            start,
            end,
            variables_sequence,
        )
        validate_resampling_interval(resampling_interval)

        pages = self._timeseries_api_client.get_paginated(
            "timeseries",
            timeseries_api_models.TimeseriesData,
            params={
                "appliance": appliance_uuid,
                "start_datetime": start.isoformat(),
                "end_datetime": end.isoformat(),
                "variables": ",".join(variables_query_parameter_list),
                "resampling_interval": resampling_interval,
            },
        )

        timeseries_data = _get_timeseries_data_from_pages(pages)
        if timeseries_data is None:
            return None

        return timeseries_data.to_user_model(
            start=start,
            end=end,
            variables=variables_sequence,
        )

    def _get_production_runs(
        self,
        *,
        production_order: Optional[str] = None,
        product: Optional[str] = None,
        appliance: Optional[UUID] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> Iterator[ProductionRun]:
        """Get production runs from the API."""

        filters = {
            "production_order": production_order,
            "product": product,
            "appliance": appliance,
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
        appliance: Optional[user_models.Appliance] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> user_models.ProductionRuns:
        """Retrieve optionally filtered list of :ref:`production runs <production_run>`.

        :param appliance: The appliance for which to get all production runs.
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
        appliances_by_uuid = {a.uuid: a for a in self.get_appliances()}
        return user_models.ProductionRuns(
            [
                production_run.to_user_model(appliances_by_uuid)
                for production_run in self._get_production_runs(
                    appliance=appliance.uuid if appliance else None,
                    production_order=production_order,
                    product=product_filter,
                    start=start,
                    end=end,
                )
            ]
        )
