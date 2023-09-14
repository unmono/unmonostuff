import contextlib
import os
import sqlite3
from typing import Generator, Any, Type, Callable
from logging import Logger, getLogger, StreamHandler, DEBUG, warning

from .utils import (
    register_sqlite_converters,
    register_sqilte_adapters,
)
from .user import BaseUser
from .exceptions import UserClassAttributeError


class SQLiteUserManager:
    """
    Supposed to manage user entries in SQLite database.
    Dedicated to use in lightweight backends, e.g. embeded, IOT etc.
    Relies only on builtins. No additional requirements needed.

    Key features:
        - forms the user table schema based on userclass(subclass of BaseUser)
        - can create new database and user table or use existed ones if its schema corresponds to user class
        - uses provided logger if needed
        - you can add your adapters and converters functions to deal with special or unserializable instances
          of user class(for example relationships)

    Features I want to add:
        - like dict functionality to be able to use hardcoded dict in early dev stage and then replace it
          with this manager
        - lazy database connection, creation, validation etc
    """

    def __init__(
        self,
        user_class: Type[BaseUser],
        database_path: str | bytes | os.PathLike = './users.sqlite',
        table_name: str | None = 'users',
        logger: Logger | None = None,
        adapters: list[tuple[Any, Callable]] | None = None,
        converters: list[tuple[str, Callable[[bytes], Any]]] | None = None,
    ):
        self.user_class = user_class
        self.database_path = database_path
        self.table_name = table_name
        self.logger = logger
        self.adapters = adapters
        self.converters = converters

        self.register_types()
        self.prepare_db()

    def add_user(self, user: BaseUser) -> Any:
        keys, values = user.insert_keys_and_values
        keys_str = ', '.join(keys)
        values_placeholders = ', '.join(['?'] * len(values))
        stmt = f'''
            INSERT INTO {self.table_name}({keys_str}) 
            VALUES ({values_placeholders});
        '''
        with self._db_connection() as db:
            cur: sqlite3.Cursor = db.execute(stmt, values)
            if user.without_rowid():
                user.pk = getattr(user, user.pk_column())
            else:
                user.pk = cur.lastrowid
                if user.pk_column() != 'rowid':
                    setattr(user, user.pk_column(), cur.lastrowid)

        return user

    def get(self, lookup: Any, default: Any = None) -> Any:
        kwargs = {
            self.user_class.lookup_field(): lookup,
        }
        users = self.get_users_by(**kwargs)
        if not users:
            return default
        if len(users) > 1:
            raise UserClassAttributeError('Defined lookup field is not unique')
        return users[0]

    def get_user_by_pk(self, pk: Any) -> Any:
        kwargs = {
            self.user_class.pk_column(): pk,
        }
        users = self.get_users_by(**kwargs)
        if not users:
            return None
        if len(users) > 1:
            raise UserClassAttributeError('Defined pk column is not unique')
        return users[0]

    def get_all_users(self) -> list[Any]:
        return self.get_users_by()

    def get_users_by(self, and_or: int = 0, **kwargs) -> list[Any]:
        fields_to_query = self.user_class.select_keys()
        where_clause, values = '', ()
        if kwargs:
            where_clause = 'WHERE ' + [' AND ', ' OR '][and_or].join([f'{col_name} = ?' for col_name in kwargs.keys()])
            values = tuple(kwargs.values())
        with self._db_connection() as db:
            cur: sqlite3.Cursor = db.execute(f'''
                SELECT {', '.join(fields_to_query)} 
                FROM {self.table_name} 
                {where_clause};
            ''', values)
            user_data = cur.fetchall()
        return [self.user_class(**ud) for ud in user_data]

    def delete_user(self, pk: Any):
        with self._db_connection() as db:
            db.execute(f'''
                DELETE FROM {self.table_name}
                WHERE {self.user_class.pk_column()} = ?;
            ''', (pk, ))

    def update_user(self, user_object: BaseUser) -> None:
        # todo: check if user exists?
        keys_and_placeholders, values = user_object.update_pairs
        with self._db_connection() as db:
            db.execute(f'''
                UPDATE {self.table_name}
                SET {', '.join(keys_and_placeholders)}
                WHERE {user_object.pk_column()} = ?;
            ''', (*values, user_object.pk))

    def prepare_db(self) -> None:
        """
        Creates table if it doesn't exist and run schema validation
        """
        schema_statement = self.make_schema_statement()
        with self._db_connection() as db:
            db.execute(schema_statement)
            if not self.validate_schema(db):
                raise sqlite3.IntegrityError(
                    'User class schema differs from the existed one in db'
                )
        if self.logger:
            self.logger.info('User database is set up')

    def validate_schema(self, db: sqlite3.Connection) -> bool:
        """
        Checks whether table in db has suitable schema for user class.
        Checks only column names and WITHOUT ROWID flag.
        Reasons for it:
            - sqlite dynamic typing
            - integrity errors aren't in scope of this class and are raised further
        :param db: sqlite3.Connection object
        """

        # Query saved schema to check WITHOUT ROWID flag
        schema_stmt = f'SELECT sql from sqlite_master WHERE `name` = ?;'
        cursor = db.execute(schema_stmt, (self.table_name, ))
        table_schema: str = cursor.fetchone()[0].lower()
        without_rowid = 'without rowid' in table_schema

        # Query single row to get set of column names
        columns_stmt = f'SELECT * FROM {self.table_name} LIMIT 1;'
        cursor.execute(columns_stmt)
        column_names = {column[0] for column in cursor.description}
        estimated_column_names = {column[0] for column in self.user_class.schema()}

        return without_rowid == self.user_class.without_rowid() and column_names == estimated_column_names

    def make_schema_statement(self):
        """
        Creates CREATE TABLE statement based on user class
        :return: CREATE TABLE statement
        """
        schema = self.user_class.schema()
        fields_str = ', '.join([' '.join(f) for f in schema])
        without_rowid = 'WITHOUT ROWID' if self.user_class.without_rowid() else ''
        return f'CREATE TABLE IF NOT EXISTS {self.table_name} ({fields_str}) {without_rowid};'

    def register_types(self) -> None:
        """
        Register provided adapters/converters.
        https://docs.python.org/3/library/sqlite3.html#how-to-adapt-custom-python-types-to-sqlite-values
        """
        if self.adapters is not None:
            register_sqilte_adapters(self.adapters)
        if self.converters is not None:
            register_sqlite_converters(self.converters)

    @contextlib.contextmanager
    def _db_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Yelds sqlite3.connection object in case of need to use its methods, e.g. operate transactions
        :return: https://docs.python.org/3/library/sqlite3.html#connection-objects
        """
        connect_kwargs = {
            'database': self.database_path,
        }
        # If custom converters are provided, sqlite3 will parse datatypes according to them.
        # https://docs.python.org/3/library/sqlite3.html#how-to-convert-sqlite-values-to-custom-python-types
        if self.converters is not None:
            connect_kwargs['detect_types'] = sqlite3.PARSE_DECLTYPES

        conn = sqlite3.connect(**connect_kwargs)
        # db requests return Row objects:
        # https://docs.python.org/3/library/sqlite3.html#row-objects
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.commit()
            conn.close()
