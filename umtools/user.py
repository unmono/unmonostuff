import dataclasses
from dataclasses import dataclass, Field, field, fields
from abc import ABC, abstractmethod
from typing import Any

from .utils import get_datatype
from .exceptions import UserClassAttributeError

import logging

def um_field(
    goes_to_db: bool = True,
    rowid_pseudo: bool = False,
    lookup_field: bool = False,
    datatype: str | None = None,
    definition: str | None = None,
    **kwargs,
) -> Field:
    """
    Helper function to insert needed attributes in dataclasses.Field.metadata

    Please read about:
        - SQLite dynamic typing: https://www.sqlite.org/datatype3.html
        - sqlite3 module type adaptation and convertion:
          https://docs.python.org/3/library/sqlite3.html#how-to-adapt-custom-python-types-to-sqlite-values

    :param goes_to_db: pass False if you don't want this attribute in database
    :param rowid_pseudo: if you want to use sequential primary key but need to use
                         name other than 'rowid' pass True
    :param datatype: datatype name that you want to use in column definition
    :param definition: column constraint if needed
    :param lookup_field: column used to
    :param kwargs: dataclasses.field() arguments
    :return: dataclasses.Field instance
    """
    metadata = {
        'goes_to_db': goes_to_db,
        'rowid_pseudo': rowid_pseudo,
        'lookup_field': lookup_field,
    }
    if datatype is not None:
        metadata['datatype'] = datatype
    if definition is not None:
        metadata['definition'] = definition

    return field(metadata=metadata, **kwargs)


@dataclass(kw_only=True)
class BaseUser(ABC):
    """
    Base class for user class in order to use it with SQLiteUserManager.
    User class has to be declared as @dataclass with kw_only=True.

    Only dataclass.Field instances with metadata['goes_to_db'] == True are used
    in user table schema in db. Use um_field function, see its docs.

    IDE's warning that 'Non-default argument(s) follows default argument(s)'
    can be ingored as long as kw_only=True flag is used.
    """
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
    def _get_db_fields(cls) -> list[Field]:
        db_fields: list[Field] = []
        for f in fields(cls):
            if not f.metadata.get('goes_to_db'):
                continue
            db_fields.append(f)
        if not db_fields:
            raise UserClassAttributeError('No database fields defined')
        return db_fields

    @classmethod
    def pk_column(cls) -> str:
        """
        Determines which column is used as primary key
        :return: name of a primary key column
        """
        for f in cls._get_db_fields():
            definition = f.metadata.get('definition')
            if definition is not None and 'primary key' in definition.lower():
                return f.name
            if cls.without_rowid():
                raise UserClassAttributeError('No primary key defined among user class attributes')
        return 'rowid'

    @classmethod
    def lookup_field(cls) -> str:
        for f in cls._get_db_fields():
            if f.metadata.get('lookup_field'):
                return f.name
        return cls.pk_column()

    @classmethod
    def schema(cls) -> list[tuple[str, ...]]:
        """
        :return: list of tuples with column name, column datatype, column definition.
                 E.g. ('name', 'TEXT', 'UNIQUE')
        """
        schema = []
        for f in cls._get_db_fields():
            name = f.name
            datatype = get_datatype(f)
            definition = f.metadata.get('definition', '')
            schema.append(
                (name, datatype, definition)
            )
        return schema

    @classmethod
    def select_keys(cls) -> list[str]:
        """
        :return: list of column names for SELECT queries
        """
        keys = [f'{cls.pk_column()} AS pk', ]
        for f in cls._get_db_fields():
            keys.append(f.name)
        return keys

    @property
    def lookup_value(self) -> Any:
        return getattr(self, self.lookup_field())

    @property
    def insert_keys_and_values(self) -> tuple[list[str], list[Any]]:
        """
        :return: list of column names and list of values for INSERT query
        """
        keys, values = [], []
        for f in self._get_db_fields():
            keys.append(f.name)
            values.append(getattr(self, f.name))
        return keys, values

    @property
    def update_pairs(self):
        """
        :return: list of column names with value placeholders('?') and list of
                 values for UPDATE query
        """
        keys_and_placeholders, values = [], []
        for f in self._get_db_fields():
            keys_and_placeholders.append(f'{f.name} = ?')
            values.append(getattr(self, f.name))
        return keys_and_placeholders, values
