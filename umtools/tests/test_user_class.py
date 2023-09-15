import unittest
import dataclasses
import datetime
from uuid import UUID, uuid4

from umtools.user import UserBaseModel, um_field


@dataclasses.dataclass(kw_only=True)
class RowidUser(UserBaseModel):
    name: str = um_field(datatype='TEXT', lookup_field=True)
    last_name: str = um_field()
    tel: int = um_field(definition='UNIQUE')


class RowidUserTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.user = RowidUser(
            name='Hey',
            last_name='Arnold',
            tel=12345,
        )
        self.pk_column = 'rowid'
        self.lookup_field = 'name'
        self.schema = [
            ('name', 'TEXT', ''),
            ('last_name', 'str', ''),
            ('tel', 'int', 'UNIQUE'),
        ]
        self.select_keys = [
            'rowid AS pk',
            'name',
            'last_name',
            'tel',
        ]
        self.insert_keys_and_values = (
            ['name', 'last_name', 'tel'],
            ['Hey', 'Arnold', 12345]
        )
        self.update_pairs = (
            ['name = ?', 'last_name = ?', 'tel = ?'],
            ['Hey', 'Arnold', 12345]
        )

    def test_pk_column(self):
        self.assertEqual(self.user.pk_column(), self.pk_column)

    def test_lookup_field(self):
        self.assertEqual(self.user.lookup_field(), self.lookup_field)

    def test_schema(self):
        self.assertEqual(self.user.schema(), self.schema)

    def test_select_keys(self):
        self.assertEqual(self.user.select_keys(), self.select_keys)

    def test_insert_keys_and_values(self):
        self.assertEqual(self.user.insert_keys_and_values, self.insert_keys_and_values)

    def test_updates_pairs(self):
        self.assertEqual(self.user.update_pairs, self.update_pairs)


@dataclasses.dataclass(kw_only=True)
class UUIDUser(UserBaseModel):
    uuid: UUID = um_field(default_factory=uuid4, definition='PRIMARY KEY')
    name: str = um_field()
    tel: int = um_field(definition='UNIQUE')

    @classmethod
    def without_rowid(cls) -> bool:
        return True


class UUIDUserTestCase(RowidUserTestCase):
    def setUp(self) -> None:
        self.user = UUIDUser(
            name='Arnold',
            tel=12345,
        )
        self.pk_column = 'uuid'
        self.lookup_field = 'uuid'
        self.schema = [
            ('uuid', 'UUID', 'PRIMARY KEY'),
            ('name', 'str', ''),
            ('tel', 'int', 'UNIQUE'),
        ]
        self.select_keys = [
            'uuid AS pk',
            'uuid',
            'name',
            'tel',
        ]
        self.insert_keys_and_values = (
            ['uuid', 'name', 'tel'],
            [self.user.uuid, 'Arnold', 12345]
        )
        self.update_pairs = (
            ['uuid = ?', 'name = ?', 'tel = ?'],
            [self.user.uuid, 'Arnold', 12345]
        )

    def test_uuid_pk(self):
        self.assertIsInstance(self.user.uuid, UUID)


@dataclasses.dataclass(kw_only=True)
class RowidAlias(UserBaseModel):
    id: int | None = um_field(datatype='INTEGER', definition='PRIMARY KEY ASC', default=None)
    name: str = um_field(datatype='TEXT', lookup_field=True)
    non_schema_field: datetime.datetime = dataclasses.field(init=False, default_factory=datetime.datetime.now)


class RowidAliasTestCase(RowidUserTestCase):
    def setUp(self) -> None:
        self.user = RowidAlias(
            name='Arnold',
        )
        self.pk_column = 'id'
        self.lookup_field = 'name'
        self.schema = [
            ('id', 'INTEGER', 'PRIMARY KEY ASC'),
            ('name', 'TEXT', ''),
        ]
        self.select_keys = [
            'id AS pk',
            'id',
            'name',
        ]
        self.insert_keys_and_values = (
            ['id', 'name'],
            [None, 'Arnold'],
        )
        self.update_pairs = (
            ['id = ?', 'name = ?'],
            [None, 'Arnold'],
        )
