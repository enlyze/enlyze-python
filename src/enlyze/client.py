from datetime import datetime
from functools import cache
from typing import Iterator, Optional, Sequence
from uuid import UUID

import enlyze.models as user_models
import enlyze.timeseries_api.models as api_models
from enlyze.constants import VARIABLE_UUID_AND_RESAMPLING_METHOD_SEPARATOR
from enlyze.errors import EnlyzeError
from enlyze.timeseries_api.client import TimeseriesApiClient
from enlyze.validators import (
    validate_resampling_interval,
    validate_timeseries_arguments,
)


def _get_timeseries_data_from_pages(
    pages: Iterator[api_models.TimeseriesData],
) -> Optional[api_models.TimeseriesData]:
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

    def __init__(self, token: str) -> None:
        self._client = TimeseriesApiClient(token=token)

    def _get_sites(self) -> Iterator[api_models.Site]:
        """Get all sites from the API"""
        return self._client.get_paginated("sites", api_models.Site)

    @cache
    def get_sites(self) -> list[user_models.Site]:
        """Retrieve all :ref:`sites <site>` of your organization.

        :raises: |token-error|

        :raises: |generic-error|

        :returns: Sites of your organization
        :rtype: list[:class:`~enlyze.models.Site`]

        """
        return [site.to_user_model() for site in self._get_sites()]

    def _get_appliances(self) -> Iterator[api_models.Appliance]:
        """Get all appliances from the API"""
        return self._client.get_paginated("appliances", api_models.Appliance)

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

    def _get_variables(self, appliance_uuid: UUID) -> Iterator[api_models.Variable]:
        """Get variables for an appliance from the API."""
        return self._client.get_paginated(
            "variables", api_models.Variable, params={"appliance": str(appliance_uuid)}
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

        :param start: Beginning of the time frame for which to fetch timeseries data.
            Must not be before ``end``.
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

        pages = self._client.get_paginated(
            "timeseries",
            api_models.TimeseriesData,
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
        variables: Sequence[user_models.VariableWithResamplingMethod],
        resampling_interval: int,
    ) -> Optional[user_models.TimeseriesData]:
        """Get timeseries data of :ref:`variables <variable>` for a given time frame.

        Timeseries data for multiple variables can be requested at once. However, all
        variables must belong to the same appliance.

        You should always pass :ref:`timezone-aware datetime
        <python:datetime-naive-aware>` objects to this method! If you don't, naive
        datetime objects will be assumed to be expressed in the local timezone of the
        system where the code is run.

        :param start: Beginning of the time frame for which to fetch timeseries data.
            Must not be before ``end``.
        :param end: End of the time frame for which to fetch timeseries data.
        :param variables: The variables for which to fetch timeseries data. These
            variables must be of the type
            :class:`~enlyze.models.VariableWithResamplingMethod`, in case you have
            variables of type :class:`~enlyze.models.Variable` you can use
            :func:`~enlyze.resampling.convert_to_variable_with_resampling_method` to
            convert each of them to
            :class:`~enlyze.models.VariableWithResamplingMethod`.
        :param resampling_interval: The interval in seconds to resample timeseries data
            with. Must be greater than or equal
            :const:`~enlyze.constants.MINIMUM_RESAMPLING_INTERVAL`.

        :raises: |token-error|

        :raises: |resampling-error|

        :raises: |generic-error|

        :returns: Timeseries data or ``None`` if the API returned no data for the
            request

        """

        start, end, appliance_uuid = validate_timeseries_arguments(
            start, end, variables
        )
        validate_resampling_interval(resampling_interval)

        pages = self._client.get_paginated(
            "timeseries",
            api_models.TimeseriesData,
            params={
                "appliance": appliance_uuid,
                "start_datetime": start.isoformat(),
                "end_datetime": end.isoformat(),
                "variables": ",".join(
                    f"{v.uuid}"
                    f"{VARIABLE_UUID_AND_RESAMPLING_METHOD_SEPARATOR}"
                    f"{v.resampling_method}"
                    for v in variables
                ),
                "resampling_interval": resampling_interval,
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
