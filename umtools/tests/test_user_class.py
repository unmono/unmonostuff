import dataclasses
import datetime
# import pytest
from uuid import UUID, uuid4

from umtools.user import BaseUser, um_field
from umtools.exceptions import UserClassAttributeError


@dataclasses.dataclass(kw_only=True)
class RowidUser(BaseUser):
    name: str = um_field(datatype='TEXT', lookup_field=True)
    last_name: str = um_field()
    tel: int = um_field(definition='UNIQUE')

    def is_superuser(self) -> bool:
        return False


@dataclasses.dataclass(kw_only=True)
class UUIDUser(BaseUser):
    uuid: UUID = um_field(default_factory=uuid4, definition='PRIMARY KEY')
    name: str = um_field()
    tel: int = um_field(definition='UNIQUE')

    def is_superuser(self) -> bool:
        return False

    @classmethod
    def without_rowid(cls) -> bool:
        return True


@dataclasses.dataclass(kw_only=True)
class RowidPseudo(BaseUser):
    id: int | None = um_field(rowid_pseudo=True, datatype='INTEGER', definition='PRIMARY KEY ASC', default=None)
    name: str = um_field(datatype='TEXT')
    non_schema_field: datetime.datetime = dataclasses.field(init=False, default_factory=datetime.datetime.now)

    def is_superuser(self) -> bool:
        return False


class Department:
    def __init__(self, dep_code: int, dep_name: str):
        self.dep_code = dep_code
        self.dep_name = dep_name


dep1 = Department(100, 'Peasants')
dep2 = Department(101, 'HR department')

departments = {
    dep1.dep_code: dep1,
    dep2.dep_code: dep2
}


@dataclasses.dataclass(kw_only=True)
class NeedsAdapters(BaseUser):
    name: str = um_field(datatype='TEXT')
    department: Department = um_field()

    def is_superuser(self) -> bool:
        return False


# @pytest.mark.parametrize(
#     'user_class, pk_column, schema, select_keys',
#     [
#         (
#             RowidUser,
#             'rowid',
#             [('name', 'TEXT', ''), ('last_name', 'str', ''), ('tel', 'int', 'UNIQUE')],
#             ['rowid AS pk', 'name', 'last_name', 'tel']
#         ),
#         (
#             UUIDUser,
#             'uuid',
#             [('uuid', 'UUID', 'PRIMARY KEY'), ('name', 'str', ''), ('tel', 'int', 'UNIQUE')],
#             ['uuid AS pk', 'uuid', 'name', 'tel']
#         ),
#         (
#             RowidPseudo,
#             'id',
#             [('id', 'INTEGER', 'PRIMARY KEY ASC'), ('name', 'TEXT', '')],
#             ['id AS pk', 'id', 'name']
#         ),
#         (
#             NeedsAdapters,
#             'rowid',
#             [('name', 'TEXT', ''), ('department', 'Department', '')],
#             ['rowid AS pk', 'name', 'department']
#         )
#     ]
# )
# def test_user_class(user_class: type[BaseUser], pk_column, schema, select_keys):
#     assert user_class.pk_column() == pk_column
#     assert user_class.schema() == schema
#     assert user_class.select_keys() == select_keys
#
#
# @dataclasses.dataclass(kw_only=True)
# class NoFields(BaseUser):
#     name: str
#     tel: int = dataclasses.field(default=1234)
#
#
# @dataclasses.dataclass(kw_only=True)
# class NoPKUser(RowidUser):
#
#     @classmethod
#     def without_rowid(cls) -> bool:
#         return True
#
#
# @pytest.mark.parametrize(
#     'user_class, expected_exception, text_to_match',
#     [
#         (NoFields, UserClassAttributeError, 'No database fields'),
#         (NoPKUser, UserClassAttributeError, 'primary key'),
#     ]
# )
# def text_user_class_exceptions(
#         user_class: type[BaseUser],
#         expected_exception: type[Exception],
#         text_to_match: str
# ):
#     with pytest.raises(expected_exception, match=text_to_match):
#         user_class.pk_column()
