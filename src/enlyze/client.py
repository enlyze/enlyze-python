import logging
from datetime import datetime, timezone
from functools import cache
from typing import Iterator, Optional
from uuid import UUID

import enlyze.models as user_models
import enlyze.timeseries_api.models as api_models
from enlyze.errors import EnlyzeError
from enlyze.timeseries_api.client import TimeseriesApiClient


def _ensure_datetime_aware(dt: datetime) -> datetime:
    """Make the returned datetime timezone aware.

    Naive datetime will be assumed to be in local timezone and then converted to aware
    datetime expressed in UTC.

    """

    return dt.astimezone(timezone.utc)


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
    ) -> list[user_models.Variable]:
        """Retrieve all variables of an :ref:`appliance <appliance>`.

        :param appliance: The appliance for which to get all variables.
        :type appliance: :class:`~enlyze.models.Appliance`

        :raises: |token-error|

        :raises: |generic-error|

        :returns: Variables of ``appliance``
        :rtype: list[:class:`~enlyze.models.Variable`]

        """
        return [
            variable.to_user_model(appliance)
            for variable in self._get_variables(appliance.uuid)
        ]

    def get_timeseries(
        self, start: datetime, end: datetime, variables: list[user_models.Variable]
    ) -> Optional[user_models.TimeseriesData]:
        """Get timeseries data of :ref:`variables <variable>` for a given time frame.

        Timeseries data for multiple variables can be requested at once. However, all
        variables must belong to the same appliance.

        You should always pass :ref:`timezone-aware datetime
        <python:datetime-naive-aware>` objects to this method! If you don't, naive
        datetime objects will be assumed to be expressed in the local timezone of the
        system where the code is run.

        :param start: Beginning of the time frame for which to fetch timeseries data.
            Must not be in the future and must be before ``end``.
        :param end: End of the time frame for which to fetch timeseries data. Must not
            be in the future.
        :param variables: The variables for which to fetch timeseries data.

        :raises: |token-error|

        :raises: |generic-error|

        :returns: Timeseries data or ``None`` if API returned no data for the request

        """

        if start.utcoffset() is None or end.utcoffset() is None:
            logging.warning(
                "Passing naive datetime is discouraged, assuming local timezone."
            )

        start = _ensure_datetime_aware(start)
        end = _ensure_datetime_aware(end)

        if end > datetime.now(tz=timezone.utc):
            raise EnlyzeError("Cannot request timeseries data in the future")

        if start > end:
            raise EnlyzeError("Start must be earlier than end")

        appliance_uuids = frozenset(v.appliance.uuid for v in variables)

        if not appliance_uuids:
            raise EnlyzeError("Need to request at least one variable")

        if len(appliance_uuids) != 1:
            raise EnlyzeError(
                "Cannot request timeseries data for more than one appliance per request"
            )

        pages = self._client.get_paginated(
            "timeseries",
            api_models.TimeseriesData,
            params={
                "appliance": str(next(iter(appliance_uuids))),
                "start_datetime": start.isoformat(),
                "end_datetime": end.isoformat(),
                "variables": ",".join(str(v.uuid) for v in variables),
            },
        )

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

        return timeseries_data.to_user_model(
            start=start,
            end=end,
            variables=variables,
        )
