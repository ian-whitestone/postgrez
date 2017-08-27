from .postgrez import Connection, Cmd
from .logger import create_logger

log = create_logger(__name__)

def query(setup, query, many=False):
    """A wrapper function around Cmd.execute() that returns formatted
    results.
    Args:
        setup (str): Name of the db setup to use in ~/.postgrez

    Raises:
        Exception: If any error occurs during execution or reading of resultset.
    """
    results = None
    try:
        with Cmd(setup) as c:
            results = c.execute(query)
    except Exception as e:
        log.error('Unable to execute and return results from query due to '
                     'err: %s' % e)
    return results
