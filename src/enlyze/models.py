from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from enum import Enum
from itertools import chain
from typing import Any, Iterator, Optional, Sequence
from uuid import UUID

import pandas


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

    #: Stable identifier of the appliance."""
    uuid: UUID

    #: Display name of the appliance."""
    display_name: str

    #: The date when the appliance has been connected to the ENLYZE platform."""
    genesis_date: date

    #: The site where the appliance is located."""
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

    #: Beginning of the requested time frame.
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
class Quantities:
    """Container of computed or synced quantities produced."""

    #: The physical unit of the quantities.
    unit: Optional[str]

    #: The total quantity of product produced. This is the sum of scrap and yield.
    total: Optional[float]

    #: The quantity of product produced that has met the quality criteria.
    yield_: Optional[float]

    #: The quantity of scrap produced.
    scrap: Optional[float]


@dataclass(frozen=True)
class Metrics:
    """Container of computed (productivity) metrics."""

    #: Productivity percentage calculated by the ENLYZE Platform.
    productivity_percentage: Optional[float]

    #: The aggregate unproductive time in seconds due to scrap production,
    #  downtimes and producing slower than the golden run.
    productivity_timeloss: Optional[int]

    #: Availability percentage calculated by the ENLYZE Platform.
    availability_percentage: Optional[float]

    #: The number of seconds lost due to downtimes.
    availability_timeloss: Optional[int]

    #: Performance percentage calculated by the ENLYZE Platform.
    performance_percentage: Optional[float]

    #: The number of seconds lost due to lower throughput compared to the golden run.
    performance_timeloss: Optional[int]

    #: Quality percentage calculated by the ENLYZE Platform.
    quality_percentage: Optional[float]

    #: The number of seconds lost due to producing scrap.
    quality_timeloss: Optional[int]


@dataclass(frozen=True)
class ProductionRun:
    """Representation of an :ref:`production run <production run>` in the ENLYZE platform.

    Contains details about the production run.

    """

    #: The UUID of the appliance the production run was exeucted on.
    appliance: UUID

    #: The average throughput of the production run excluding downtimes.
    average_throughput: Optional[float]

    #: The identifier of the production order.
    production_order: str

    #: The identifier of the product that was produced.
    product: str

    #: The begin of the production run.
    begin: datetime

    #: The end of the production run.
    end: Optional[datetime]

    #: Quantities produced during the production run.
    quantities: Optional[Quantities] = Quantities()

    #: Productivity metrics of the production run
    metrics: Optional[Metrics] = Metrics()


class ProductionRuns(list):
    """Representation of multiple :ref:`production runs <production run>`"""

    def to_dataframe(self) -> pandas.DataFrame:
        """Convert production runs into :py:class:`pandas.DataFrame`

        Each row in the data frame will represent one production run.
        The ``begin`` and ``end`` of every production run will be
        represented as :ref:`timezone-aware <python:datetime-naive-aware>`
        :py:class:`datetime.datetime` localized in UTC.

        :returns: DataFrame with production runsthat consists of

        """

        df = pandas.json_normalize([asdict(run) for run in self])
        df.begin = pandas.to_datetime(df.begin, utc=True, format="ISO8601")
        df.end = pandas.to_datetime(df.end, utc=True, format="ISO8601")
        return df
