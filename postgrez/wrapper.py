from .postgrez import Connection, Cmd, QUERY_LENGTH
from .logger import create_logger

log = create_logger(__name__)

def query(setup, query, query_vars=None, columns=True, many=False):
    """A wrapper function around Cmd.execute() that returns formatted
    results.

    Args:
        setup (str): Name of the db setup to use in ~/.postgrez
        query_vars (tuple, list or dict): Variables to be executed with query.
            See http://initd.org/psycopg/docs/usage.html#query-parameters.
        query (str): Query to be executed. Query can contain placeholders,
            as long as query_vars are supplied.
        columns (bool): Return column names in results. Defaults to True.
    Returns:
        results (list): Results from query.
            Returns None if no resultset was generated (i.e. insert into
            query, update query etc..).

    Raises:
        PostgrezExecuteError: If any error occurs during execution
            or reading of resultset.
    """
    results = None
    try:
        with Cmd(setup) as c:
            c.execute(query, query_vars)
            # no way to check if results were returned other than try-except
            try:
                results = c.cursor.fetchall()
            except psycopg2.ProgrammingError as e:
                # this error is raised when there are no results to fetch
                pass

            if columns and results:
                cols = [desc[0] for desc in c.cursor.description]
                results = [{cols[i]:value for i, value in enumerate(row)}
                            for row in results]
    except Exception as e:
        raise PostgrezExecuteError('Unable to execute query %s due to '
                     'Error: %s' % (query[0:QUERY_LENGTH], e))
    return results
