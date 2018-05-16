"""
Exceptions module, contains custom Postgrez exceptions.
Reference: https://www.programiz.com/python-programming/user-defined-exception
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


class PostgrezConnectionError(Postgrez):
    """Raised when a function is called that requires a connection, but no
        connection is present.
    """


class PostgrezExecuteError(Postgrez):
    """Raised when there is an error fetching results from the cursor..
    """


class PostgrezLoadError(Postgrez):
    """Raised when there is an error running the Load.load_from() functions.
    """



class PostgrezExportError(Postgrez):
    """Raised when there is an error running the Export.export_to() functions.
    """
