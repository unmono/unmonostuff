import sqlite3
from typing import get_args, Any, Callable
from dataclasses import Field


def get_datatype(field: Field) -> str:
    datatype: str | None = field.metadata.get('datatype')
    if datatype is not None:
        return datatype
    if get_args(field.type):
        raise TypeError('If you use union type for an attribute, you have to provide \'datatype\' argument.')
    return field.type.__name__


def register_sqilte_adapters(adapters: list[tuple[Any, Callable[[Any], str | bytes]]]) -> None:
    for py_type, adapter in adapters:
        sqlite3.register_adapter(py_type, adapter)


def register_sqlite_converters(converters: list[tuple[str, Callable[[bytes], Any]]]) -> None:
    for col_definition, converter in converters:
        sqlite3.register_converter(col_definition, converter)
