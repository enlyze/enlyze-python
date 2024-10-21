from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import DataclassInstance
import typing
from dataclasses import is_dataclass
from types import UnionType

import pandas


def _flat_dataclass_schema(
    dataclass_obj_or_type: DataclassInstance | type[DataclassInstance],
    path_separator: str,
    _parent_path: list[str] = [],
) -> list[str]:
    """Derive flat schema of potentially nested dataclass ``dataclass_obj_or_type``"""

    flat: list[str] = []

    for field, typ in typing.get_type_hints(dataclass_obj_or_type).items():
        current_path = _parent_path + [field]
        field_types = (typ,)

        # expand union types (includes typing.Optional)
        origin_type = typing.get_origin(typ)
        if origin_type is UnionType or origin_type is typing.Union:
            field_types = typing.get_args(typ)

        for field_type in field_types:
            if is_dataclass(field_type):
                flat.extend(
                    _flat_dataclass_schema(field_type, path_separator, current_path)
                )
            elif field_type is not type(None):
                flat.append(path_separator.join(current_path))

    # dedupe while preserving order
    return list(dict.fromkeys(flat))


def dataframe_ensure_schema(
    df: pandas.DataFrame,
    dataclass_obj_or_type: DataclassInstance | type[DataclassInstance],
    path_separator: str = ".",
) -> pandas.DataFrame:
    """Add missing columns to ``df`` based on flattened dataclass schema"""

    flat_schema = _flat_dataclass_schema(
        dataclass_obj_or_type,
        path_separator=path_separator,
    )

    add_colums = set(flat_schema) - set(df.columns)
    remove_columns = set(df.columns) - set(flat_schema)

    return df.assign(
        **{col: None for col in add_colums},
    ).drop(
        columns=list(remove_columns),
    )
