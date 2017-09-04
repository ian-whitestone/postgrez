"""Main postgrez module
Module contains 4 core classes: Connection, Cmd, Export and Load.
"""

import psycopg2
from .utils import read_yaml, IteratorFile
from .exceptions import (PostgrezConfigError, PostgrezConnectionError,
                            PostgrezExecuteError, PostgrezLoadError,
                            PostgrezExportError)
from .logger import create_logger
import os
import sys
import re
import io


log = create_logger(__name__)

## number of characters in query to display
QUERY_LENGTH = 50

class Connection(object):
    """Class which establishes connections to a PostgresSQL database.

    Methods used internally by the class are prefixed with a `_`.

    Attributes:
        host (str): Database host address
        port (int): Connection port number (defaults to 5432 if not provided)
        database (str): Name of the database
        user (str): Username used to authenticate
        password (str, optional): Password used to authenticate
        conn (psycopg2 connection): psycopg2 connection object
        cursor (psycopg2 cursor): psycopg2 cursor object, associated with
            the connection object
    """
    def __init__(self, setup):
        """Initialize connection to postgres database.
        Args:
            setup (str): Name of the db setup to use in ~/.postgrez
        """
        self.setup = setup
        self.host = None
        self.database = None
        self.user = None
        self.password = None
        self.port = None
        self.conn = None
        self.cursor = None

        ## Fetch attributes from file
        self._get_attributes()

        ## Validate the parsed attributes
        self._validate_attributes()

        ## If no errors are raised, connect to the database
        self._connect()

    def _get_attributes(self):
        """Read database connection parameters from ~/.postgrez.

        Raises:
            PostgrezConfigError: If the config file ~/.postgrez does not exist.
            PostgrezConfigError: If the supplied setup variable is not in the
                ~/.postgrez file

        """
        yaml_file = os.path.join(os.path.expanduser('~'), '.postgrez')

        if os.path.isfile(yaml_file) == False:
            raise PostgrezConfigError('Unable to find ~/.postgrez config file')

        config = read_yaml(yaml_file)

        if self.setup not in config.keys():
            raise PostgrezConfigError('Setup variable %s not found in config '
                                        'file' % self.setup)

        self.host = config[self.setup].get('host', None)
        self.port = config[self.setup].get('port', 5432)
        self.database = config[self.setup].get('database', None)
        self.user = config[self.setup].get('user', None)
        self.password = config[self.setup].get('password', None)

    def _validate_attributes(self):
        """Validate that the minimum required fields were parsed from
            ~/.postgrez

        Raises:
            PostgrezConfigError: If the minimum attributes were not included
                in ~/.postgrez.
        """

        if self.host is None or self.user is None or self.database is None:
            raise PostgrezConfigError('Please provide a host, user and '
                'database as a minimum in ~/.postgrez. Please visit '
                'https://github.com/ian-whitestone/postgrez for details')

    def _connected(self):
        """Determine if a pscyopg2 connection or cursor has been created.

        Returns:
            connect_status (bool): True of a psycopg2 connection or cursor
                object exists.
        """
        return (True if self.conn or self.cursor else False)

    def _connect(self):
        """Create a connection to a PostgreSQL database.

        Raises:
            Exception: If there is a problem creating a connection.
        """
        try:
            log.info('Establishing connection to %s database' % self.database)
            self.conn = psycopg2.connect(host=self.host,
                                    port=self.port,
                                    database=self.database,
                                    user=self.user,
                                    password=self.password)
            self.cursor = self.conn.cursor()
        except Exception as e:
            log.error('Error connecting to database. Error: %s', e)
            raise

    def _disconnect(self):
        """Create a connection to a PostgreSQL database.

        Raises:
            Exception: If there is a problem creating a connection.
        """
        log.info('Attempting to disconnect from database %s' % self.database)
        try:
            self.cursor.close()
        except Exception as e:
            log.error('Error closing cursor. Error: %s', e)
            raise
        try:
            self.conn.close()
        except Exception as e:
            log.error('Error closing connection. Error: %s', e)
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        """Close the cursor and connection objects if they have been created.
        This code is automatically executed after the with statement is
        completed or if any error arises during the process.
        Ref: https://stackoverflow.com/questions/1984325/explaining-pythons-enter-and-exit
        """
        if self._connected():
            log.info("Attempting to close connection ...")
            self._disconnect()
            log.info("Connection closed")


class Cmd(Connection):
    """Class which handles execution of queries.
    """

    def __init__(self, setup):
        """Initialize connection to postgres database.

        Args:
            setup (str): Name of the db setup to use in ~/.postgrez
        """
        super(Cmd, self).__init__(setup)

    def execute(self, query, query_vars=None, commit=True, columns=True):
        """Execute the supplied query.

        Args:
            query (str): Query to be executed. Query can contain placeholders,
                as long as query_vars are supplied.
            query_vars (tuple, list or dict): Variables to be executed with query.
                See http://initd.org/psycopg/docs/usage.html#query-parameters.
            commit (bool): Commit any pending transaction to the database.
                Defaults to True.

        Raises:
            PostgrezConnectionError: If no connection has been established.
            PostgrezExecuteError: If any error occurs during execution of query.
        """
        if self._connected() == False:
            raise PostgrezConnectionError('No connection has been established')

        log.info('Executing query %s...' % query[0:QUERY_LENGTH].strip())
        try:
            self.cursor.execute(query, vars=query_vars)

            if commit:
                self.conn.commit()

        except Exception as e:
            raise PostgrezExecuteError('Unable to execute query %s due to '
                         'Error: %s' % (query[0:QUERY_LENGTH], e))


class Load(Connection):
    """Class which handles loading data functionality.
    """

    def __init__(self, setup):
        """Initialize connection to postgres database.

        Args:
            setup (str): Name of the db setup to use in ~/.postgrez
        """
        super(Load, self).__init__(setup)


    def load_from_object(self, table_name, data):
        """Load data into a Postgres table from a python list.

        Args:
            table_name (str): name of table to load data into.
            data (list): list of tuples, where each row is a tuple

        Raises:
            Exception: If an error occurs while loading.
        """
        try:
            log.info('Attempting to load %s records into table %s' %
                        (len(data), table_name))

            table_width = len(data[0])
            template_string = "|".join(['{}'] * table_width)
            f = IteratorFile((template_string.format(*x) for x in data))
            self.cursor.copy_from(f, table_name, sep="|", null='NULL')
            self.conn.commit()

        except Exception as e:
            raise PostgrezLoadError("Unable to load data to Postgres. "
                "Error: %s" % e)


    def load_from_file(self, table_name, filename, delimiter=','):
        """
        Args:
            table_name (str): name of table to load data into.
            filename (str): name of the file
            delimiter (str): delimiter with which the columns are separated.
                Defaults to ','

        Raises:
            Exception: If an error occurs while loading.
        """
        try:
            log.info('Attempting to load file %s  into table %s' %
                        (filename, table_name))

            with open(filename, 'r') as f:
                self.cursor.copy_from(f, table, sep=delimiter, null='NULL')
                self.conn.commit()

        except Exception as e:
            raise PostgrezLoadError("Unable to load file to Postgres. "
                "Error: %s" % e)

class Export(Connection):
    """Class which handles exporting data.
    """

    def __init__(self, setup):
        """Initialize connection to postgres database.

        Args:
            setup (str): Name of the db setup to use in ~/.postgrez
        """
        super(Export, self).__init__(setup)


    def _build_copy_query(self, query, columns=None, delimiter=',', header=True):
        """Build query used in the cursor.copy_expert() method.

        Args:
            query (str): A select query or a table
            columns (list): List of column names to export. columns should only
                be provided if you are exporting a table
                (i.e. query = 'table_name'). If query is a query to export, desired
                columns should be specified in the select portion of that query
                (i.e. query = 'select col1, col2 from ...'). Defaults to None.
            delimiter (str): Delimiter to separate columns with. Defaults to ','.
            header (boolean): Specify True to return the column names. Defaults
                to True.

        Returns:
            copy_query (str): Formatted query to run in copy_expert()

        Raises:
            Exception: If an error occurs while building hte query.
        """
        try:
            log.info('Building copy query')
            if columns:
                columns = '(' + ','.join(columns) + ')'

            ## check if provided query is a query, or just a table name
            if re.match('\s*select', query, re.IGNORECASE):
                if columns:
                    log.warning('If a query is passed in the query arg '
                                'instead of a tablename, columns must be '
                                'specified in the query itself')
                    columns = None
                copy_query = "COPY ({0}) {1} TO STDOUT WITH DELIMITER '{2}' " \
                                " CSV {3}"
            else:
                copy_query = "COPY {0} {1} TO STDOUT WITH DELIMITER '{2}' " \
                                " CSV {3}"

            copy_query = copy_query.format(
                query,
                (columns if columns else ''),
                delimiter,
                ('HEADER' if header else '')
                )
            return copy_query
        except Exception as e:
            raise PostgrezExportError('Unable to build query. Error: %s' % (e))

    def export_to_file(self, query, filename, columns=None, delimiter=',',
                header=True):
        """Export records from a table or query to a local file.

        Args:
            query (str): A select query or a table
            columns (list): List of column names to export. columns should only
                be provided if you are exporting a table
                (i.e. query = 'table_name'). If query is a query to export, desired
                columns should be specified in the select portion of that query
                (i.e. query = 'select col1, col2 from ...'). Defaults to None.
            filename (str): Filename to copy to.
            delimiter (str): Delimiter to separate columns with. Defaults to ','.
            header (boolean): Specify True to return the column names. Defaults
                to True.

        Raises:
            PostgrezExportError: If an error occurs while exporting to the file.
        """

        copy_query = self._build_copy_query(query, columns, delimiter, header)
        try:
            log.info('Running copy_expert with\n%s\nOutputting results to %s' %
                        (copy_query, filename))
            with open(filename, 'w') as f:
                self.cursor.copy_expert(copy_query, f)
        except Exception as e:
            raise PostgrezExportError('Unable to export to file %s. Error: %s'
                    % (filename, e))


    def export_to_object(self, query, columns=None, delimiter=',', header=True):
        """Export records from a table or query and returns list of records.

        Args:
            query (str): A select query or a table_name
            columns (list): List of column names to export. columns should only
                be provided if you are exporting a table
                (i.e. query = 'table_name'). If query is a query to export, desired
                columns should be specified in the select portion of that query
                (i.e. query = 'select col1, col2 from ...'). Defaults to None.
            delimiter (str): Delimiter to separate columns with. Defaults to ','
            header (boolean): Specify True to return the column names. Defaults
                to True.

        Returns:
            data (list): If header is True, returns list of dicts where each
                dict is in the format {col1: val1, col2:val2, ...}. Otherwise,
                returns a list of lists where each list is [val1, val2, ...].

        Raises:
            PostgrezExportError: If an error occurs while exporting to an object.
        """

        copy_query = self._build_copy_query(query, columns, delimiter, header)
        data = None
        try:
            log.info('Running copy_expert with with\n%s\nOutputting results to '
                     'list.' % copy_query)
            # stream output to local object
            text_stream = io.StringIO()
            self.cursor.copy_expert(copy_query, text_stream)
            output = text_stream.getvalue()

            # parse output
            output = output.split('\n')
            cols = output[0].split(delimiter)
            end_index = (-1 if len(output[1:]) > 1 else 2)
            if header:
                data = [{cols[i]:value for i, value in
                            enumerate(row.split(delimiter))}
                            for row in output[1:end_index]]
            else:
                data = [row.split(delimiter) for row in output[1:-1]]
        except Exception as e:
            raise PostgrezExportError('Unable to export to object. Error: %s'
                    % (e))

        return data
