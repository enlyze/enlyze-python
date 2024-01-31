import typing
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import is_dataclass
from types import UnionType
from typing import Any, ClassVar, Protocol

import pandas


class IsDataclass(Protocol):
    __dataclass_fields__: dict[str, Any]


# def get_optional_dataclass_fields(obj: IsDataclass) -> set[str]:
#     """Extract optional dataclass fields from ``obj``."""
#     optional_fields = set()
#     for field, typ in typing.get_type_hints(obj).items():
#         # typing.Optional[T] is represented as typing.Union[None, T]
#         if typing.get_origin(typ) != UnionType:
#             continue

#         union_args = list(typing.get_args(typ))

#         try:
#             union_args.remove(type(None))
#         except ValueError:
#             # None wasn't part of Union, so not optional
#             continue

#         if any(is_dataclass(arg) for arg in union_args):
#             optional_fields.add(field)

#     return optional_fields


# def _dict_deep_merge(dict_a: dict[Any, Any], dict_b: dict[Any, Any]) -> dict[Any, Any]:
#     """Deep merge two dictionaries."""
#     merged_dict = deepcopy(dict_a)

#     for key in dict_b:
#         if (
#             key in merged_dict
#             and isinstance(merged_dict[key], dict)
#             and isinstance(dict_b[key], Mapping)
#         ):
#             merged_dict[key] = _dict_deep_merge(merged_dict[key], dict_b[key])
#         else:
#             merged_dict[key] = dict_b[key]

#     return merged_dict


# def _dataclass_schema(obj: IsDataclass) -> dict[str, Any]:
#     schema: dict[str, Any] = {}

#     for field, typ in typing.get_type_hints(obj).items():
#         field_types = (typ,)

#         if typing.get_origin(typ) == UnionType:
#             field_types = typing.get_args(typ)

#         for field_type in field_types:
#             if is_dataclass(field_type):
#                 field_schema = _dataclass_schema(field_type)
#             else:
#                 field_schema = {}

#             print(f"{field}: ({field_type}) -> {field_schema}")
#             schema = _dict_deep_merge(schema, {field: field_schema})

#     return schema


# def _flatten_schema(
#     schema: dict[str, Any],
#     separator: str = "_",
#     path: list[str] = [],
# ) -> list[str]:
#     flat_schema: list[str] = []

#     for field, sub_schema in schema.items():
#         current_path = path + [field]
#         if sub_schema:
#             flat_schema.extend(
#                 _flatten_schema(
#                     schema=sub_schema,
#                     separator=separator,
#                     path=current_path,
#                 )
#             )
#         else:
#             flat_schema.append(separator.join(current_path))

#     return flat_schema


def _immediate_flat_dataclass_schema(
    obj: IsDataclass,
    path_separator: str,
    path: list[str] = [],
) -> set[str]:
    flat: set[str] = set()

    for field, typ in typing.get_type_hints(obj).items():
        current_path = path + [field]

        field_types = (typ,)
        if typing.get_origin(typ) == UnionType:
            field_types = typing.get_args(typ)

        for field_type in field_types:
            if is_dataclass(field_type):
                sub_schema = _immediate_flat_dataclass_schema(
                    field_type, path_separator, current_path
                )
                flat.update(sub_schema)
            elif field_type != type(None):
                flat.add(path_separator.join(current_path))

    return flat


def dataframe_ensure_schema(
    df: pandas.DataFrame,
    obj: IsDataclass,
    path_separator: str = ".",
) -> pandas.DataFrame:
    """Derive flat schema of nested dataclass ``obj`` and add missing columns to ``df``"""

    flat_schema = _immediate_flat_dataclass_schema(obj, path_separator=path_separator)
    missing_columns = set(flat_schema) - set(df.columns)

    return df.assign(**{col: None for col in missing_columns})
