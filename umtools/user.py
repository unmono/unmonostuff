from dataclasses import dataclass, Field, field, fields
from abc import ABC, abstractmethod
from typing import Any

from utils import get_datatype


def um_field(
    ignore_in_schema: bool = False,
    rowid_pseudo: bool = False,
    datatype: str | None = None,
    definition: str | None = None,
    **kwargs,
) -> Field:
    metadata = {
        'goes_to_db': True,
        'ignore_in_schema': ignore_in_schema,
        'rowid_pseudo': rowid_pseudo,
    }
    if datatype is not None:
        metadata['datatype'] = datatype
    if definition is not None:
        metadata['definition'] = definition

    return field(metadata=metadata, **kwargs)


@dataclass(kw_only=True)
class BaseUser(ABC):
    pk: Any = None

    @property
    @abstractmethod
    def is_superuser(self) -> bool:
        """
        Provide your method to determine wether the user is super
        """
        pass

    @classmethod
    def without_rowid(cls) -> bool:
        """
        Overwrite this method if you want to use schema without default sqlite rowid.
        You don't need to overwrite it if you plan to use sequential integer primary
        key with just another name instead of rowid.
        """
        return False

    @classmethod
    def pk_column(cls) -> str:
        for f in fields(cls):
            definition = f.metadata.get('definition')
            if definition is not None and 'primary key' in definition.lower():
                return f.name
        return 'rowid'

    @classmethod
    def schema(cls) -> list[tuple[str, ...]]:
        schema = []
        user_fields = fields(cls)
        for f in user_fields:
            if not f.metadata.get('goes_to_db'):
                continue
            name = f.name
            datatype = get_datatype(f)
            definition = f.metadata.get('definition', '')
            schema.append(
                (name, datatype, definition)
            )
        return schema

    @classmethod
    def select_keys(cls) -> list[str]:
        keys = [f'{cls.pk_column()} AS pk', ]
        for f in fields(cls):
            if not f.metadata.get('goes_to_db'):
                continue
            keys.append(f.name)
        return keys

    @property
    def insert_keys_and_values(self) -> tuple[list[str], list[Any]]:
        keys, values = [], []
        for f in fields(self):
            if not f.metadata.get('goes_to_db'):
                continue
            keys.append(f.name)
            values.append(getattr(self, f.name))
        return keys, values

    @property
    def update_pairs(self):
        keys_and_placeholders, values = [], []
        for f in fields(self):
            if not f.metadata.get('goes_to_db'):
                continue
            keys_and_placeholders.append(f'{f.name} = ?')
            values.append(getattr(self, f.name))
        return keys_and_placeholders, values
