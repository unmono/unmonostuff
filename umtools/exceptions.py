class UserAlreadyExists(Exception):
    pass


class UserDoesNotExist(Exception):
    pass


class DatabaseCorrupted(Exception):
    pass


class UserClassAttributeError(AttributeError):
    pass
