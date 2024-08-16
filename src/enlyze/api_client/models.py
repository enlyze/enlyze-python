from datetime import date, datetime, timedelta
from typing import Any, Optional, Sequence
from uuid import UUID

from pydantic import BaseModel

import enlyze.models as user_models


class PlatformApiModel(BaseModel):
    """Base class for Enlyze Platform API object models using pydantic

    All objects received from the Enlyze Platform API are passed into models
    that derive from this class and thus use pydantic for schema definition
    and validation.

    """


class Site(PlatformApiModel):
    uuid: UUID
    name: str
    address: str

    def to_user_model(self) -> user_models.Site:
        """Convert into a :ref:`user model <user_models>`"""

        return user_models.Site(
            uuid=self.uuid,
            address=self.address,
            display_name=self.name,
        )


class MachineBase(PlatformApiModel):
    """The machine related information returned for a
    :class:`.ProductionRun`"""

    name: str
    uuid: UUID


class Machine(MachineBase):
    genesis_date: date
    site: UUID

    def to_user_model(self, site: user_models.Site) -> user_models.Machine:
        """Convert into a :ref:`user model <user_models>`"""

        return user_models.Machine(
            uuid=self.uuid,
            display_name=self.name,
            genesis_date=self.genesis_date,
            site=site,
        )


class Variable(PlatformApiModel):
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


class TimeseriesData(PlatformApiModel):
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


class OEEComponent(PlatformApiModel):
    score: float
    time_loss: int

    def to_user_model(self) -> user_models.OEEComponent:
        """Convert into a :ref:`user model <user_models>`"""

        return user_models.OEEComponent(
            score=self.score,
            time_loss=timedelta(seconds=self.time_loss),
        )


class Product(PlatformApiModel):
    code: str
    name: Optional[str]

    def to_user_model(self) -> user_models.Product:
        """Convert into a :ref:`user model <user_models>`"""

        return user_models.Product(
            code=self.code,
            name=self.name,
        )


class Quantity(PlatformApiModel):
    unit: str | None
    value: float

    def to_user_model(self) -> user_models.Quantity:
        """Convert into a :ref:`user model <user_models>`"""

        return user_models.Quantity(
            unit=self.unit,
            value=self.value,
        )


class ProductionRun(PlatformApiModel):
    uuid: UUID
    machine: MachineBase
    average_throughput: Optional[float]
    production_order: str
    product: Product
    start: datetime
    end: Optional[datetime]
    quantity_total: Optional[Quantity]
    quantity_scrap: Optional[Quantity]
    quantity_yield: Optional[Quantity]
    availability: Optional[OEEComponent]
    performance: Optional[OEEComponent]
    quality: Optional[OEEComponent]
    productivity: Optional[OEEComponent]

    def to_user_model(
        self, machines_by_uuid: dict[UUID, user_models.Machine]
    ) -> user_models.ProductionRun:
        """Convert into a :ref:`user model <user_models>`"""

        quantity_total = (
            self.quantity_total.to_user_model() if self.quantity_total else None
        )
        quantity_scrap = (
            self.quantity_scrap.to_user_model() if self.quantity_scrap else None
        )
        quantity_yield = (
            self.quantity_yield.to_user_model() if self.quantity_yield else None
        )
        availability = self.availability.to_user_model() if self.availability else None
        performance = self.performance.to_user_model() if self.performance else None
        quality = self.quality.to_user_model() if self.quality else None
        productivity = self.productivity.to_user_model() if self.productivity else None

        return user_models.ProductionRun(
            uuid=self.uuid,
            machine=machines_by_uuid[self.machine.uuid],
            average_throughput=self.average_throughput,
            production_order=self.production_order,
            product=self.product.to_user_model(),
            start=self.start,
            end=self.end,
            quantity_total=quantity_total,
            quantity_scrap=quantity_scrap,
            quantity_yield=quantity_yield,
            availability=availability,
            performance=performance,
            quality=quality,
            productivity=productivity,
        )
