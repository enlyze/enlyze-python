from datetime import date, datetime
from typing import Any, Optional, Sequence
from uuid import UUID

import enlyze.models as user_models
from enlyze.api_clients.base import ApiBaseModel


class TimeseriesApiModel(ApiBaseModel):
    """Base class for Timeseries API object models using pydantic

    All objects received from the Timeseries API are passed into models that derive from
    this class and thus use pydantic for schema definition and validation.

    """

    pass


class Site(TimeseriesApiModel):
    id: int
    name: str
    address: str

    def to_user_model(self) -> user_models.Site:
        """Convert into a :ref:`user model <user_models>`"""

        return user_models.Site(
            _id=self.id,
            address=self.address,
            display_name=self.name,
        )


class Machine(TimeseriesApiModel):
    uuid: UUID
    name: str
    genesis_date: date
    site: int

    def to_user_model(self, site: user_models.Site) -> user_models.Machine:
        """Convert into a :ref:`user model <user_models>`"""

        return user_models.Machine(
            uuid=self.uuid,
            display_name=self.name,
            genesis_date=self.genesis_date,
            site=site,
        )


class Variable(TimeseriesApiModel):
    uuid: UUID
    display_name: Optional[str]
    unit: Optional[str]
    data_type: user_models.VariableDataType

    def to_user_model(self, machine: user_models.Machine) -> user_models.Variable:
        """Convert into a :ref:`user model <user_models>`."""

        return user_models.Variable(
            uuid=self.uuid,
            display_name=self.display_name,
            unit=self.unit,
            data_type=self.data_type,
            machine=machine,
        )


class TimeseriesData(TimeseriesApiModel):
    columns: list[str]
    records: list[Any]

    def extend(self, other: "TimeseriesData") -> None:
        """Add records from ``other`` after the existing records."""
        self.records.extend(other.records)

    def merge(self, other: "TimeseriesData") -> "TimeseriesData":
        """Merge records from ``other`` into the existing records."""
        slen, olen = len(self.records), len(other.records)
        if olen < slen:
            raise ValueError(
                "Cannot merge. Attempted to merge"
                f" an instance with {olen} records into an instance with {slen}"
                " records. The instance to merge must have a number"
                " of records greater than or equal to the number of records of"
                " the instance you're trying to merge into."
            )

        self.columns.extend(other.columns[1:])

        for s, o in zip(self.records, other.records[:slen]):
            if s[0] != o[0]:
                raise ValueError(
                    "Cannot merge. Attempted to merge records "
                    f"with mismatched timestamps {s[0]}, {o[0]}"
                )

            s.extend(o[1:])

        return self

    def to_user_model(
        self,
        start: datetime,
        end: datetime,
        variables: Sequence[user_models.Variable],
    ) -> user_models.TimeseriesData:
        return user_models.TimeseriesData(
            start=start,
            end=end,
            variables=variables,
            _columns=self.columns,
            _records=self.records,
        )
