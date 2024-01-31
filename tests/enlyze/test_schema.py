from dataclasses import dataclass

import pandas

from enlyze.schema import dataframe_ensure_schema  # get_optional_dataclass_fields,


@dataclass
class Some:
    a: int


@dataclass
class Thing:
    number: int
    maybe_string: str | None
    maybe_some: Some | None
    multiple_but_required: float | str | Some


# def test_get_optional_dataclass_fields():
#     assert get_optional_dataclass_fields(Thing) == {"maybe_some"}


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


# def test_immediate_flat():
#     assert _immediate_flat_dataclass_schema(Thing, path_separator="|") == {
#         "number",
#         "maybe_string",
#         "maybe_some|a",
#         "multiple_but_required",
#         "multiple_but_required|a",
#     }
