from .postgrez import Connection, Cmd, Load, Export, QUERY_LENGTH
from .logger import create_logger

log = create_logger(__name__)

def execute(setup, query, query_vars=None, columns=True):
    """A wrapper function around Cmd.execute() that returns formatted
    results.

    Args:
        setup (str): Name of the db setup to use in ~/.postgrez
        query (str): Query to be executed. Query can contain placeholders,
        as long as query_vars are supplied.
        query_vars (tuple, list or dict): Variables to be executed with query.
            See http://initd.org/psycopg/docs/usage.html#query-parameters.
        columns (bool): Return column names in results. Defaults to True.

    Returns:
        results (list): Results from query.
            Returns None if no resultset was generated (i.e. insert into
            query, update query etc..).

    Raises:
        PostgrezExecuteError: If any error occurs reading of resultset.
    """
    results = None

    with Cmd(setup) as c:
        c.execute(query, query_vars)
        # no way to check if results were returned other than try-except
        try:
            results = c.cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            # this error is raised when there are no results to fetch
            pass

        try:
            if columns and results:
                cols = [desc[0] for desc in c.cursor.description]
                results = [{cols[i]:value for i, value in enumerate(row)}
                        for row in results]
        except Exception as e:
            raise PostgrezExecuteError('Unable to retrieve results query %s '
                         '.Error: %s' % (query[0:QUERY_LENGTH], e))
    return results


def load(setup, table_name, filename=None, data=None, delimiter=',',
            columns=None, null=None):
    """A wrapper function around Load.load_from methods. If a filename is
    provided, the records will loaded from that file. Otherwise, records
    will be loaded from the supplied data arg.

    Args:
        table_name (str): name of table to load data into.
        filename (str, optional): name of the file. Defaults to None.
        data (list, optional): list of tuples, where each row is a tuple.
            Defaults to None.
        delimiter (str, optional): If a filename is provided, delimiter with
            which the columns are separated can be specified. Defaults to ','
        columns (list): iterable with name of the columns to import.
            The length and types should match the content of the file to
            read. If not specified, it is assumed that the entire table
            matches the file structure. Defaults to None.
        null (str): Format which nulls (or missing values) are represented.
                Defaults to '' if a file is provided, 'None' if an object is
                provided. See a more detailed explanation in the
                Load.load_from_file() or Load.load_from_object() functions.
    """
    if data is None and filename is None:
        log.warning('No filename or data object was supplied. Exiting...')
        return

    with Load(setup) as l:
        if filename:
            if null:
                l.load_from_file(table_name, filename, delimiter=delimiter,
                                    columns=columns, null=null)
            else:
                l.load_from_file(table_name, filename, delimiter=delimiter,
                                    columns=columns)
        else:
            if null:
                l.load_from_object(table_name, data, columns=columns, null=null)
            else:
                l.load_from_object(table_name, data, columns=columns)



def export(setup, query, filename=None, columns=None, delimiter=',',
            header=True):
    """A wrapper function around Export.export_to methods. If a filename is
    provided, the records will be written to that file. Otherwise, records
    will be returned.

    Args:
        setup (str): Name of the db setup to use in ~/.postgrez
        query (str): A select query or a table_name
        filename (str, optional): Filename to copy to. Defaults to None.
        columns (list): List of column names to export. columns should only
            be provided if you are exporting a table
            (i.e. query = 'table_name'). If query is a query to export, desired
            columns should be specified in the select portion of that query
            (i.e. query = 'select col1, col2 from ...'). Defaults to None.
        delimiter (str): Delimiter to separate columns with. Defaults to ','
        header (boolean): Specify True to return the column names. Defaults
            to True.

    Returns:
        data (list): If noe filename is provided, records will be returned.
            If header is True, returns list of dicts where each
            dict is in the format {col1: val1, col2:val2, ...}. Otherwise,
            returns a list of lists where each list is [val1, val2, ...].
    """
    data = None
    with Export(setup) as e:
        if filename:
            e.export_to_file(query, filename=filename, columns=columns,
                                delimiter=delimiter, header=header)
        else:
            data = e.export_to_object(query, columns=columns,
                                        delimiter=delimiter, header=header)
    return data
