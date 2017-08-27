"""Reference: https://www.programiz.com/python-programming/user-defined-exception
"""


class Postgrez(Exception):
    """Base class for Postgrez errors.
    """
    pass


class PostgrezConfigError(Postgrez):
    """Raised when there is an error with the postgrez config file
        ~/.postgrez
    """
    pass
