import collections
import typing
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from itertools import chain
from typing import Any, Iterator, Optional, Sequence
from uuid import UUID

import pandas


def _get_optional_dataclass_fields(cls: object) -> set[str]:
    hints = typing.get_type_hints(cls)
    optional_fields = set()
    for field, typ in hints.items():
        if typing.get_origin(typ) != typing.Union:
            continue

        args = list(typing.get_args(typ))
        try:
            args.remove(type(None))
        except ValueError:
            continue

        try:
            (nested_type,) = args
        except ValueError:
            continue

        if is_dataclass(nested_type):
            optional_fields.add(field)

    return optional_fields


def dict_merge(dct, merge_dct):
    dct = dct.copy()

    for key in merge_dct:
        if (
            key in dct
            and isinstance(dct[key], dict)
            and isinstance(merge_dct[key], collections.abc.Mapping)
        ):
            dct[key] = dict_merge(dct[key], merge_dct[key])
        else:
            dct[key] = merge_dct[key]

    return dct


def dataclass_schema(cls):
    schema = {}
    hints = typing.get_type_hints(cls)
    for field, typ in hints.items():
        field_types = [typ]

        if typing.get_origin(typ) == typing.Union:
            field_types = typing.get_args(typ)

        for field_type in field_types:
            if is_dataclass(field_type):
                field_schema = dataclass_schema(field_type)
            else:
                field_schema = {}

            schema = dict_merge(schema, {field: field_schema})

    return schema


def flatten_schema(schema: dict, separator: str = "_", level=None) -> list[str]:
    flat_schema = []
    for field, subschema in schema.items():
        current_level = (level or []) + [field]
        if subschema:
            flat_schema.extend(
                flatten_schema(subschema, separator, level=current_level)
            )
        else:
            flat_schema.append(separator.join(current_level))

    return flat_schema


def dataframe_ensure_schema(df: pandas.DataFrame, obj: object) -> pandas.DataFrame:
    schema = dataclass_schema(obj)
    flat_schema = flatten_schema(schema, separator=".")

    missing_cols = set(flat_schema) - set(df.columns)

    return df.assign(**{col: None for col in missing_cols})


@dataclass(frozen=True)
class Site:
    """Representation of a :ref:`site <site>` in the ENLYZE platform.

    Contains details about the site.

    """

    _id: int

    #: Display name of the site.
    display_name: str

    #: Postal address of the site. Doesn't follow a strict format.
    address: str


@dataclass(frozen=True)
class Appliance:
    """Representation of an :ref:`appliance <appliance>` in the ENLYZE platform.

    Contains details about the appliance.

    """

    #: Stable identifier of the appliance.
    uuid: UUID

    #: Display name of the appliance.
    display_name: str

    #: The date when the appliance has been connected to the ENLYZE platform.
    genesis_date: date

    #: The site where the appliance is located.
    site: Site


class VariableDataType(str, Enum):
    """Enumeration of variable data types. Compares to strings out-of-the-box:

    .. code-block:: pycon

        >>> VariableDataType.INTEGER == 'INTEGER'
        True

    """

    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    STRING = "STRING"
    ARRAY_INTEGER = "ARRAY_INTEGER"
    ARRAY_FLOAT = "ARRAY_FLOAT"
    ARRAY_BOOLEAN = "ARRAY_BOOLEAN"
    ARRAY_STRING = "ARRAY_STRING"


class ResamplingMethod(str, Enum):
    """Resampling method to be used when resampling timeseries data."""

    FIRST = "first"
    LAST = "last"
    MAX = "max"
    MIN = "min"
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MEDIAN = "median"


@dataclass(frozen=True)
class Variable:
    """Representation of a :ref:`variable <variable>` in the ENLYZE platform.

    Contains details about the variable, but no timeseries data.

    """

    #: Stable identifier of the variable.
    uuid: UUID

    #: Display name of the variable.
    display_name: Optional[str]

    #: Unit of the measure that the variable represents.
    unit: Optional[str]

    #: The underlying data type of the variable.
    data_type: VariableDataType

    #: The appliance on which this variable is read out.
    appliance: Appliance


@dataclass(frozen=True)
class TimeseriesData:
    """Result of a request for timeseries data."""

    #: Start of the requested time frame.
    start: datetime

    #: End of the requested time frame.
    end: datetime

    #: The variables for which timeseries data has been requested.
    variables: Sequence[Variable]

    _columns: list[str]
    _records: list[Any]

    def __len__(self) -> int:
        """Returns the number of resulting rows"""
        return len(self._records)

    def __repr__(self) -> str:
        """Summary of what was requested and how many records were retrieved"""
        return (
            "TimeseriesData: {:d} variables, {:d} records, start='{}' end='{}'".format(
                len(self.variables),
                len(self),
                self.start.isoformat(" "),
                self.end.isoformat(" "),
            )
        )

    def _display_names_as_column_names(self, columns: list[str]) -> list[str]:
        uuid_to_display_name = {
            str(var.uuid): var.display_name
            for var in self.variables
            if var.display_name
        }

        return [uuid_to_display_name.get(var_uuid, var_uuid) for var_uuid in columns]

    def to_dicts(self, use_display_names: bool = False) -> Iterator[dict[str, Any]]:
        """Convert timeseries data into rows of :py:class:`dict`.

        Each row is returned as a dictionary with variable UUIDs as keys. Additionally,
        the ``time`` column is always present, containing :ref:`timezone-aware
        <python:datetime-naive-aware>` :py:class:`datetime.datetime` localized in UTC.

        :param use_display_names: Whether to return display names instead of variable
            UUIDs. If there is no display name fall back to UUID.

        :returns: Iterator over rows

        """

        time_column, *variable_columns = self._columns

        if use_display_names:
            variable_columns = self._display_names_as_column_names(variable_columns)

        for record in self._records:
            time_tuple = (
                time_column,
                datetime.fromisoformat(record[0]).astimezone(timezone.utc),
            )
            variable_tuples = zip(variable_columns, record[1:])
            yield dict(chain([time_tuple], variable_tuples))

    def to_dataframe(self, use_display_names: bool = False) -> pandas.DataFrame:
        """Convert timeseries data into :py:class:`pandas.DataFrame`

        The data frame will have an index named ``time`` that consists of
        :ref:`timezone-aware <python:datetime-naive-aware>`
        :py:class:`datetime.datetime` localized in UTC. Each requested variables is
        represented as a column named by its UUID.

        :param use_display_names: Whether to return display names instead of variable
            UUIDs. If there is no display name fall back to UUID.

        :returns: DataFrame with timeseries data indexed by time

        """

        time_column, *variable_columns = self._columns

        if use_display_names:
            variable_columns = self._display_names_as_column_names(variable_columns)

        df = pandas.DataFrame.from_records(
            self._records,
            columns=[time_column] + variable_columns,
            index="time",
        )
        df.index = pandas.to_datetime(df.index, utc=True, format="ISO8601")
        return df


@dataclass(frozen=True)
class OEEComponent:
    """Individual Overall Equipment Effectiveness (OEE) score

    This is calculated by the ENLYZE Platform based on a combination of real machine
    data and production order booking information provided by the customer.

    For more information, please check out https://www.oee.com

    """

    #: The score is expressed as a ratio between 0 and 1.0, with 1.0 meaning 100 %.
    score: float

    #: Unproductive time due to non-ideal production.
    time_loss: timedelta


@dataclass(frozen=True)
class Quantity:
    """Representation of a physical quantity"""

    #: Physical unit of quantity
    unit: str | None

    #: The quantity expressed in `unit`
    value: float


@dataclass(frozen=True)
class Product:
    """Representation of a product that is produced on an appliance"""

    #: The identifier of the product
    code: str

    #: An optional human-friendly name of the product
    name: Optional[str] = None


@dataclass(frozen=True)
class ProductionRun:
    """Representation of a production run in the ENLYZE platform.

    Contains details about the production run.

    """

    #: The UUID of the production run
    uuid: UUID

    #: The appliance the production run was executed on.
    appliance: Appliance

    #: The average throughput of the production run excluding downtimes.
    average_throughput: Optional[float]

    #: The identifier of the production order.
    production_order: str

    #: The identifier of the product that was produced.
    product: Product

    #: The begin of the production run.
    start: datetime

    #: The end of the production run.
    end: Optional[datetime]

    #: This is the sum of scrap and yield.
    quantity_total: Optional[Quantity]

    #: The amount of product produced that doesn't meet quality criteria.
    quantity_scrap: Optional[Quantity]

    #: The amount of product produced that can be sold.
    quantity_yield: Optional[Quantity]

    #: OEE component that reflects when the appliance did not produce.
    availability: Optional[OEEComponent]

    #: OEE component that reflects how fast the appliance has run.
    performance: Optional[OEEComponent]

    #: OEE component that reflects how much defects have been produced.
    quality: Optional[OEEComponent]

    #: Aggregate OEE score that comprises availability, performance and quality.
    productivity: Optional[OEEComponent]

    def to_dict(self, exclude_unset_objects: bool = False) -> dict[str, Any]:
        """Convert to Python dictionary.

        The ``start`` and ``end`` fields will be represented as
        :ref:`timezone-aware <python:datetime-naive-aware>`
        :py:class:`datetime.datetime` localized in UTC.

        :param exclude_unset_objects: Exclude fields that are typed as optional
            dataclasses and set to ``None``.

        :returns: Production run represented as Python dictionary.

        """
        data = asdict(
            self,
            dict_factory=lambda items: {
                k: v
                for k, v in items
                if not (
                    exclude_unset_objects
                    and k in _get_optional_dataclass_fields(self)
                    and v is None
                )
            },
        )

        data["start"] = data["start"].astimezone(timezone.utc)
        if data["end"]:
            data["end"] = data["end"].astimezone(timezone.utc)

        return data

    def to_dataframe(self) -> pandas.DataFrame:
        """Convert to :py:class:`pandas.DataFrame`

        The ``start`` and ``end`` fields will be represented as
        :ref:`timezone-aware <python:datetime-naive-aware>`
        :py:class:`datetime.datetime` localized in UTC.

        :returns: DataFrame with a single row representing the production run.
        """
        return pandas.json_normalize(self.to_dict(exclude_unset_objects=True))


class ProductionRuns(list[ProductionRun]):
    """Representation of multiple production runs."""

    def to_dicts(self, exclude_unset_objects: bool = False) -> list[dict[str, Any]]:
        """Convert to Python dictionaries.

        The ``start`` and ``end`` fields will be represented as
        :ref:`timezone-aware <python:datetime-naive-aware>`
        :py:class:`datetime.datetime` localized in UTC.

        :param exclude_unset_objects: Exclude fields that are typed as optional
            dataclasses and set to ``None``.

        :returns: List of Production runs represented as Python dictionaries.

        """
        return [
            run.to_dict(
                exclude_unset_objects=exclude_unset_objects,
            )
            for run in self
        ]

    def to_dataframe(self) -> pandas.DataFrame:
        """Convert production runs into :py:class:`pandas.DataFrame`

        Each row in the dataframe represents one production run. The ``start`` and
        ``end`` of every production run will be represented as :ref:`timezone-aware
        <python:datetime-naive-aware>` :py:class:`datetime.datetime` localized in UTC.

        :returns: DataFrame with production runs.
        """
        if not self:
            return pandas.DataFrame()

        df = pandas.json_normalize(self.to_dicts(exclude_unset_objects=False))

        return dataframe_ensure_schema(df, ProductionRun)
