import os
import unittest
import sqlite3
import contextlib
from typing import Generator
from pathlib import Path

from uuid import UUID, uuid4

from umtools.sqlite_um import SQLiteUserManager
from .test_user_class import RowidUser, RowidPseudo, UUIDUser, NeedsAdapters, Department, departments

import logging

def uuid_adapter(uuid_obj):
    return str(uuid_obj)


def uuid_converter(uuid_str: bytes) -> UUID:
    uuid_a = uuid_str.decode()
    return UUID(uuid_a)


def department_adapter(dep: Department):
    return dep.dep_code


def department_converter(dep_code: bytes):
    code = int(dep_code)
    return departments[code]


@contextlib.contextmanager
def db(table_name: str) -> Generator[sqlite3.Connection, None, None]:
    sqlite3.register_adapter(UUID, uuid_adapter)
    sqlite3.register_converter('uuid', uuid_converter)
    conn = sqlite3.connect(table_name, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


class BasicUserManagerTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = 'test_user_table'
        self.db_path = './test_user_database.sqlite'

    def setUp(self) -> None:
        db_path = Path(self.db_path)
        if db_path.exists():
            os.remove(self.db_path)
        self.test_user = RowidUser(
            name='test_user',
            last_name='LN',
            tel=12345,
        )
        self.um = SQLiteUserManager(
            user_class=RowidUser,
            database_path=self.db_path,
            table_name=self.table_name,
        )
        self.extras_users = [
            {
                'name': 'user1',
                'last_name': 'last name 1',
                'tel': 11111,
            },
            {
                'name': 'user2',
                'last_name': 'last name 2',
                'tel': 22222,
            },
            {
                'name': 'user3',
                'last_name': 'last name 3',
                'tel': 33333,
            },
        ]
        with db(self.db_path) as conn:
            conn.executemany(f'INSERT INTO {self.table_name} VALUES(:name, :last_name, :tel);', self.extras_users)
            conn.commit()

    def tearDown(self) -> None:
        os.remove(self.db_path)

    def test_names(self):
        self.assertTrue(Path(self.db_path).exists())
        with db(self.db_path) as conn:
            count = conn.execute(f'SELECT COUNT(*) FROM sqlite_master WHERE name=?', (self.table_name, ))
            self.assertEqual(count.fetchone()[0], 1, 'Table with specified name wasn\'t created')

    def test_add_user(self):
        user = self.um.add_user(self.test_user)
        with db(self.db_path) as conn:
            q = conn.execute(f'SELECT COUNT(*) FROM {self.table_name} WHERE {user.pk_column()} = ?;', (user.pk, ))
            self.assertEqual(q.fetchone()[0], 1, 'User wasn\'t created right')

    def test_get_user_by_pk(self):
        with db(self.db_path) as conn:
            q = conn.execute(f'SELECT {self.test_user.pk_column()} as pk, * FROM {self.table_name} LIMIT 1;')
            first_user_row = q.fetchone()
        first_user = self.um.user_class(**first_user_row)
        retrieved_user = self.um.get_user_by_pk(first_user.pk)
        self.assertEqual(first_user._get_db_fields(), retrieved_user._get_db_fields())

    def test_get_existed_user(self):
        user_obj = self.um.add_user(self.test_user)
        user = self.um.get(user_obj.lookup_value)
        self.assertEqual(self.test_user._get_db_fields(), user._get_db_fields())

    def test_get_nonexisted_user(self):
        self.assertTrue(self.um.get('non existed user', True))

    def test_get_all_users(self):
        users = self.um.get_all_users()
        self.assertEqual(len(users), 3)

    def test_delete_user(self):
        with db(self.db_path) as conn:
            q = conn.execute(f'SELECT {self.test_user.pk_column()} as pk FROM {self.table_name} LIMIT 1;')
            first_user = q.fetchone()
            self.um.delete_user(first_user['pk'])
            count = conn.execute(
                f'SELECT COUNT(*) FROM {self.table_name} WHERE {self.test_user.pk_column()} = ?;',
            (first_user['pk'], )).fetchone()[0]
        self.assertEqual(count, 0)

    def test_update_user(self):
        with db(self.db_path) as conn:
            pk = conn.execute(f'SELECT {self.test_user.pk_column()} as pk FROM {self.table_name} LIMIT 1;').fetchone()[0]
        user = self.um.get_user_by_pk(pk)
        user.name = 'different name'
        self.um.update_user(user)
        user = self.um.get_user_by_pk(pk)
        self.assertEqual(user.name, 'different name')

    def test_in(self):
        pass


class TestUUIDUserManagerTestCase(BasicUserManagerTestCase):
    def setUp(self) -> None:
        db_path = Path(self.db_path)
        if db_path.exists():
            os.remove(self.db_path)
        self.test_user = UUIDUser(
            name='test_user',
            tel=12345,
        )
        self.um = SQLiteUserManager(
            user_class=UUIDUser,
            database_path=self.db_path,
            table_name=self.table_name,
            adapters=[(UUID, uuid_adapter), ],
            converters=[('uuid', uuid_converter), ]
        )
        self.extras_users = [
            {
                'uuid': uuid4(),
                'name': 'user1',
                'tel': 11111,
            },
            {
                'uuid': uuid4(),
                'name': 'user2',
                'tel': 22222,
            },
            {
                'uuid': uuid4(),
                'name': 'user3',
                'tel': 33333,
            },
        ]
        with db(self.db_path) as conn:
            conn.executemany(f'INSERT INTO {self.table_name} VALUES(:uuid, :name, :tel);', self.extras_users)
            conn.commit()


class TestRowidPseudoUserManagerTestCase(BasicUserManagerTestCase):
    def setUp(self) -> None:
        db_path = Path(self.db_path)
        if db_path.exists():
            os.remove(self.db_path)
        self.test_user = RowidPseudo(
            name='test_user',
        )
        self.um = SQLiteUserManager(
            user_class=RowidPseudo,
            database_path=self.db_path,
            table_name=self.table_name,
        )
        self.extras_users = [
            {
                'id': None,
                'name': 'user1',
            },
            {
                'id': None,
                'name': 'user2',
            },
            {
                'id': None,
                'name': 'user3',
            },
        ]
        with db(self.db_path) as conn:
            conn.executemany(f'INSERT INTO {self.table_name} VALUES(:id, :name);', self.extras_users)
            conn.commit()


if __name__ == '__main__':
    unittest.main()
