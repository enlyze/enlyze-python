from pydantic import BaseModel


class PlatformApiModel(BaseModel):
    """Base class for Enlyze Platform API object models using pydantic

    All objects received from the Enlyze Platform API are passed into models
    that derive from this class and thus use pydantic for schema definition
    and validation.

    """

    pass
