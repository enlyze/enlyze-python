from dataclasses import dataclass

import pandas

from enlyze.schema import dataframe_ensure_schema


@dataclass
class Some:
    a: int


@dataclass
class Thing:
    number: int
    maybe_string: str | None
    maybe_some: Some | None
    multiple_but_required: float | str | Some


def test_dataframe_ensure_schema():
    df = pandas.DataFrame()

    added_columns = set(dataframe_ensure_schema(df, Thing, path_separator="|").columns)

    assert added_columns == {
        "number",
        "maybe_string",
        "maybe_some|a",
        "multiple_but_required",
        "multiple_but_required|a",
    }
