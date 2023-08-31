import dataclasses
import pytest
import os
from uuid import UUID, uuid4

from umtools.sqlite_um import um_field, BaseUser, SQLiteUserManager


@dataclasses.dataclass
class RowidUser(BaseUser):
    name: str = um_field(datatype='TEXT')
    last_name: str = um_field()
    tel: int = um_field(definition='UNIQUE')

    def is_superuser(self) -> bool:
        return False


@dataclasses.dataclass(kw_only=True)
class UUIDUser(BaseUser):
    uuid: UUID = um_field(default_factory=uuid4, definition='UNIQUE')
    name: str = um_field()
    tel: int = um_field(definition='UNIQUE')

    def is_superuser(self) -> bool:
        return False


def uuid_adapter(uuid_obj):
    return str(uuid_obj)


def uuid_converter(uuid_str):
    uuid_a = uuid_str.decode()
    return UUID(uuid_a)


um = SQLiteUserManager(
    user_class=UUIDUser,
    adapters=[(UUID, uuid_adapter), ],
    converters=[('UUID', uuid_converter), ],
)

user = um.get_user_by_pk(1)
assert False, type(user.uuid)
