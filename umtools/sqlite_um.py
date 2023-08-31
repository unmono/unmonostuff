import contextlib
import os
import sqlite3
from typing import Generator, Any, Type, Callable
from logging import Logger, getLogger, StreamHandler, DEBUG

from .utils import (
    register_sqlite_converters,
    register_sqilte_adapters,
)
from .user import BaseUser


class SQLiteUserManager:

    def __init__(
        self,
        user_class: Type[BaseUser],
        database_path: str | bytes | os.PathLike = './users.sqlite',
        table_name: str | None = 'users',
        logger: Logger | None = None,
        adapters: list[tuple[Any, Callable]] | None = None,
        converters: list[tuple[str, Callable]] | None = None,
    ):
        self.user_class = user_class
        self.database_path = database_path
        self.table_name = table_name
        self.logger = logger
        self.adapters = adapters
        self.converters = converters

        self.register_types()
        self.prepare_db()

    def add_user(self, user: BaseUser) -> BaseUser:
        keys, values = user.insert_keys_and_values
        keys_str = ', '.join(keys)
        values_placeholders = ', '.join(['?'] * len(values))
        stmt = f'''
            INSERT INTO {self.table_name}({keys_str}) 
            VALUES ({values_placeholders});
        '''
        with self._db_connection() as db:
            cur = db.execute(stmt, values)
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
        schema_stmt = f'SELECT sql from sqlite_master WHERE `name` = ?;'
        cursor = db.execute(schema_stmt, (self.table_name, ))
        table_schema: str = cursor.fetchone()[0].lower()
        without_rowid = 'without rowid' in table_schema

        columns_stmt = f'SELECT * FROM {self.table_name} LIMIT 1;'
        cursor.execute(columns_stmt)
        column_names = {column[0] for column in cursor.description}
        estimated_column_names = {column[0] for column in self.user_class.schema()}

        return without_rowid == self.user_class.without_rowid() and column_names == estimated_column_names

    def make_schema_statement(self):
        schema = self.user_class.schema()
        fields_str = ', '.join([' '.join(f) for f in schema])
        without_rowid = 'WITHOUT ROWID' if self.user_class.without_rowid() else ''
        return f'CREATE TABLE IF NOT EXISTS {self.table_name} ({fields_str}) {without_rowid};'

    def register_types(self) -> None:
        if self.adapters is not None:
            register_sqilte_adapters(self.adapters)
        if self.converters is not None:
            register_sqlite_converters(self.converters)

    @contextlib.contextmanager
    def _db_connection(self) -> Generator[sqlite3.Connection, None, None]:
        connect_kwargs = {
            'database': self.database_path,
        }
        if self.converters is not None:
            connect_kwargs['detect_types'] = sqlite3.PARSE_DECLTYPES

        conn = sqlite3.connect(**connect_kwargs)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.commit()
            conn.close()
