from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Any, Optional
from uuid import UUID

import enlyze.models as user_models
from enlyze.api_clients.base import ApiBaseModel


class ProductionRunsApiModel(ApiBaseModel):
    """Base class for Production Runs API object models using pydantic

    All objects received from the Production Runs API are passed into models that derive
    from this class and thus use pydantic for schema definition and validation.

    """

    @abstractmethod
    def to_user_model(self, *args: Any, **kwargs: Any) -> Any:
        """Convert to a model that will be returned to the user."""


class OEEComponent(ProductionRunsApiModel):
    score: float
    time_loss: int

    def to_user_model(self) -> user_models.OEEComponent:
        """Convert into a :ref:`user model <user_models>`"""

        return user_models.OEEComponent(
            score=self.score,
            time_loss=timedelta(seconds=self.time_loss),
        )


class Product(ProductionRunsApiModel):
    code: str
    name: Optional[str]

    def to_user_model(self) -> user_models.Product:
        """Convert into a :ref:`user model <user_models>`"""

        return user_models.Product(
            code=self.code,
            name=self.name,
        )


class Quantity(ProductionRunsApiModel):
    unit: str | None
    value: float

    def to_user_model(self) -> user_models.Quantity:
        """Convert into a :ref:`user model <user_models>`"""

        return user_models.Quantity(
            unit=self.unit,
            value=self.value,
        )


class Appliance(ApiBaseModel):
    name: str
    uuid: UUID


class ProductionRun(ProductionRunsApiModel):
    uuid: UUID
    appliance: Appliance
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
        self, appliances_by_uuid: dict[UUID, user_models.Appliance]
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
            appliance=appliances_by_uuid[self.appliance.uuid],
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
