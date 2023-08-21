import contextlib
import os
import sqlite3
import dataclasses
from typing import Generator, Any, Type
from abc import ABC, abstractmethod
from logging import Logger, getLogger, StreamHandler, DEBUG
from decimal import Decimal


class UserAlreadyExists(Exception):
    pass


class UserDoesNotExist(Exception):
    pass


class DatabaseCorrupted(Exception):
    pass


def um_field(
    ignore_in_schema: bool = False,
    rowid_pseudo: bool = False,
    datatype: str | None = None,
    definition: str | None = None,
    **kwargs,
) -> dataclasses.Field:
    metadata = {
        'ignore_in_schema': ignore_in_schema,
        'rowid_pseudo': rowid_pseudo,
    }
    if datatype is not None:
        metadata['datatype'] = datatype
    if definition is not None:
        metadata['definition'] = definition

    return dataclasses.field(metadata=metadata, **kwargs)


@dataclasses.dataclass
class BaseUser(ABC):
    # This field only needed when default sqlite rowid is used
    # Otherwise user defined primary key is used
    pk: Any | None = um_field(ignore_in_schema=True, default=None)

    @abstractmethod
    @property
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
    def schema(cls) -> list[tuple[str, ...]]:
        schema = []
        fields = dataclasses.fields(cls)
        for f in fields:
            if f.metadata.get('ignore_in_schema') or f.name.startswith('_'):
                continue
            name = f.name
            datatype = f.metadata.get('datatype') or f.type.__name__
            definition = f.metadata.get('definition', '')
            schema.append(
                (name, datatype, definition)
            )
        return schema

    @classmethod
    def select_keys(cls) -> list[str]:
        keys = [f'{cls.pk_column()} AS pk', ]
        for f in dataclasses.fields(cls):
            if f.metadata.get(f.metadata.get('ignore_in_schema') or f.name.startswith('_')):
                continue
            keys.append(f.name)
        return keys

    @property
    def insert_keys_and_values(self) -> tuple[list[str], list[Any]]:
        keys, values = [], []
        for f in dataclasses.fields(self):
            field_avoiding_rules = [
                f.metadata.get('ignore_in_schema'),
                f.name.startswith('_'),
                f.metadata.get('rowid_pseudo'),
            ]
            if any(field_avoiding_rules):
                continue
            keys.append(f.name)
            values.append(getattr(self, f.name))
        return keys, values

    @property
    def update_pairs(self):
        keys_and_placeholders, values = [], []
        for f in dataclasses.fields(self):
            field_avoiding_rules = [
                f.metadata.get('ignore_in_schema'),
                f.name.startswith('_'),
                f.name == self.pk_column()
            ]
            if any(field_avoiding_rules):
                continue
            keys_and_placeholders.append(f'{f.name} = ?')
            values.append(getattr(self, f.name))
        return keys_and_placeholders, values

    @classmethod
    def pk_column(cls) -> str:
        for f in dataclasses.fields(cls):
            if 'primary key' in f.metadata.get('definition').lower():
                return f.name
        return 'rowid'


class SQLiteUserManager:
    """
    Problems:
        - validates only the schema with the same order of fields.
        - if additional field is added to user class, stops validating
    """

    def __init__(
        self,
        user_class: Type[BaseUser],
        database_path: str | bytes | os.PathLike = 'users.sqlite',
        table_name: str | None = 'users',
        logger: Logger | None = None,
    ):
        self.user_class = user_class
        self.database_path = database_path
        self.table_name = table_name
        self.logger = logger

        self.prepare_db()

    def add_user(self, user: BaseUser) -> BaseUser:
        keys, values = user.insert_keys_and_values
        keys_str = ', '.join(keys)
        values_placeholders = ', '.join(['?'] * len(values))
        with self._db_connection() as db:
            cur = db.execute(f'''
                INSERT INTO {self.table_name}({keys_str}) 
                VALUES ({values_placeholders});
            ''', values)
            if not user.without_rowid():
                user.pk = cur.execute("SELECT last_insert_rowid();").fetchone()[0]
            else:
                user.pk = getattr(user, user.pk_column())
        return user

    def get_user_by_pk(self, pk: Any) -> BaseUser:
        pk_column = self.user_class.pk_column()
        fields_to_query = self.user_class.select_keys()
        with self._db_connection() as db:
            cur = db.execute(f'''
                SELECT {', '.join(fields_to_query)} 
                FROM {self.table_name}
                WHERE {pk_column} = ?;
            ''', (pk, ))
            user_data = cur.fetchone()
        return self.user_class(**user_data)

    def get_all_users(self) -> list[BaseUser]:
        fields_to_query = self.user_class.select_keys()
        with self._db_connection() as db:
            cur = db.execute(f'''
                SELECT {', '.join(fields_to_query)}
                FROM {self.table_name};
            ''')
            user_rows = cur.fetchall()
        users = [self.user_class(**ur) for ur in user_rows]
        return users

    def delete_user(self, pk: Any):
        with self._db_connection() as db:
            db.execute(f'''
                DELETE FROM {self.table_name}
                WHERE {self.user_class.pk_column()} = ?;
            ''', (pk, ))

    def update_user(self, user_object: BaseUser) -> BaseUser:
        keys_and_placeholders, values = user_object.update_pairs
        with self._db_connection() as db:
            db.execute(f'''
                UPDATE {self.table_name}
                SET {', '.join(keys_and_placeholders)}
                WHERE {user_object.pk_column()} = ?;
            ''', (*values, user_object.pk))

    def prepare_db(self) -> None:
        schema_statement = self.make_schema_statement()
        with self._db_connection() as db:
            db.execute(schema_statement)
            if not self.validate_schema(db, schema_statement):
                raise sqlite3.IntegrityError('User class schema differs from the existed one in db')
        if self.logger:
            self.logger.info('User database is set up')

    def validate_schema(self, db: sqlite3.Connection, schema_statement: str):
        db_schema = db.execute('SELECT sql from sqlite_master WHERE name = ?;', (self.table_name, ))
        return schema_statement == db_schema

    def make_schema_statement(self):
        schema = self.user_class.schema()
        fields_str = ', '.join([' '.join(f) for f in schema])
        without_rowid = 'WITHOUT ROWID' if self.user_class.without_rowid() else ''
        return f'CREATE TABLE IF NOT EXISTS {self.table_name} ({fields_str}) {without_rowid};'

    @contextlib.contextmanager
    def _db_connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.database_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
