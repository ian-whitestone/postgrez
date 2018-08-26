"""
Main postgrez module
"""

import psycopg2
from .utils import read_yaml, IteratorFile, build_copy_query
from .exceptions import (PostgrezConfigError, PostgrezConnectionError,
                            PostgrezExecuteError, PostgrezLoadError,
                            PostgrezExportError)
import os
import sys
import io
import logging

LOGGER = logging.getLogger(__name__)

## number of characters in query to display
QUERY_LENGTH = 50

## initialization defaults
DEFAULT_PORT = 5432
DEFAULT_SETUP = 'default'
DEFAULT_SETUP_PATH = '~'

class Connection(object):
    """Class which establishes connections to a PostgresSQL database. Users
    have the option to provide the host, database, username, password and port
    to connect to. Alternatively, they can utilize their .postgrez configuration
    file (recommended).

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
    def __init__(self, host=None, database=None, user=None, password=None,
                    port=DEFAULT_PORT, setup=DEFAULT_SETUP,
                    setup_path=DEFAULT_SETUP_PATH):
        """Initialize connection to postgres database. First, we look if a host,
        database, username and password were provided. If they weren't, we try
        and read credentials from the .postgrez config file.

        Args:
            host (str, optional): Database host url. Defaults to None.
            database (str, optional): Database name. Defaults to None.
            user (str, optional): Username. Defaults to None.
            password (str, optional): Password. Defaults to None.
            setup (str, optional): Name of the db setup to use in ~/.postgrez.
                If no setup is provided, looks for the 'default' key in
                ~/.postgrez which specifies the default configuration to use.
            setup_path (str, optional): Path to the .postgrez configuration
                file. Defaults to '~', i.e. your home directory on Mac/Linux.
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.setup = setup
        self.setup_path = setup_path
        self.conn = None
        self.cursor = None

        if host is None and database is None and user is None:
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
        if self.setup_path == '~':
            yaml_file = os.path.join(os.path.expanduser('~'), '.postgrez')
        else:
            yaml_file = os.path.join(self.setup_path, '.postgrez')
        LOGGER.info('Fetching attributes from .postgrez file: %s' % yaml_file)

        if os.path.isfile(yaml_file) == False:
            raise PostgrezConfigError('Unable to find ~/.postgrez config file')

        config = read_yaml(yaml_file)

        if self.setup not in config.keys():
            raise PostgrezConfigError('Setup variable %s not found in config '
                                        'file' % self.setup)

        if self.setup == 'default':
            # grab the default setup key
            self.setup = config[self.setup]

        self.host = config[self.setup].get('host', None)
        self.port = config[self.setup].get('port', 5432)
        self.database = config[self.setup].get('database', None)
        self.user = config[self.setup].get('user', None)
        self.password = config[self.setup].get('password', None)

    def _validate_attributes(self):
        """Validate that the minimum required fields were either supplied or
            parsed from the .postgrez configuration file.

        Raises:
            PostgrezConfigError: If the minimum attributes were not supplied or
            included in ~/.postgrez.
        """

        if self.host is None or self.user is None or self.database is None:
            raise PostgrezConfigError('Please provide a host, user and '
                'database as a minimum. Please visit '
                'https://github.com/ian-whitestone/postgrez for details')

    def _connected(self):
        """Determine if a pscyopg2 connection or cursor has been created.

        Returns:
            connect_status (bool): True of a psycopg2 connection or cursor
                object exists.
        """
        return (True if self.conn.closed == 1 else False)

    def _connect(self):
        """Create a connection to a PostgreSQL database.
        """
        LOGGER.info('Establishing connection to %s database' % self.database)
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
            )
        self.cursor = self.conn.cursor()

    def _disconnect(self):
        """Close connection
        """
        LOGGER.debug('Attempting to disconnect from database %s' % self.database)
        self.cursor.close()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        """Close the cursor and connection objects if they have been created.
        This code is automatically executed after the with statement is
        completed or if any error arises during the process.
        Reference: https://stackoverflow.com/questions/1984325/explaining-pythons-enter-and-exit
        """
        if self._connected():
            self._disconnect()


class Cmd(Connection):
    """Class which handles execution of queries.
    """

    def execute(self, query, query_vars=None, commit=True):
        """Execute the supplied query.

        Args:
            query (str): Query to be executed. Query can contain placeholders,
                as long as query_vars are supplied.
            query_vars (tuple, list or dict): Variables to be executed with query.
                See http://initd.org/psycopg/docs/usage.html#query-parameters.
            commit (bool): Commit any pending transaction to the database.
                Defaults to True.

        Raises:
            PostgrezConnectionError: If the connection has been closed.
        """
        if self._connected() == False:
            raise PostgrezConnectionError('Connection has been closed')

        LOGGER.info('Executing query %s...' % query[0:QUERY_LENGTH].strip())
        self.cursor.execute(query, vars=query_vars)
        if commit:
            self.conn.commit()

    def load_from_object(self, table_name, data, columns=None, null=None):
        """Load data into a Postgres table from a python list.

        Args:
            table_name (str): name of table to load data into.
            data (list): list of tuples, where each row is a tuple
            columns (list): iterable with name of the columns to import.
                The length and types should match the content of the file to
                read. If not specified, it is assumed that the entire table
                matches the file structure. Defaults to None.
            null (str): Format which nulls (or missing values) are represented.
                Defaults to 'None'. If a row is passed in as
                [None, 1, '2017-05-01', 25.321], it will treat the first
                element as missing and inject a Null value into the database for
                the corresponding column.
        Raises:
            PostgrezLoadError: If an error occurs while building the iterator
                file.
        """
        try:
            LOGGER.info('Attempting to load %s records into table %s' %
                        (len(data), table_name))
            if null is None:
                null = 'None'

            table_width = len(data[0])
            template_string = "|".join(['{}'] * table_width)
            f = IteratorFile((template_string.format(*x) for x in data))
        except Exception as e:
            raise PostgrezLoadError("Unable to load data to Postgres. "
                "Error: %s" % e)

            self.cursor.copy_from(f, table_name, sep="|", null=null,
                                    columns=columns)
            self.conn.commit()

    def load_from_file(self, table_name, filename, header=True, delimiter=',',
                        columns=None, quote=None, null=None):
        """
        Args:
            table_name (str): name of table to load data into.
            filename (str): name of the file
            header (boolean): Specify True if the first row of the flat file
                contains the column names. Defaults to True.
            delimiter (str): delimiter with which the columns are separated.
                Defaults to ','
            columns (list): iterable with name of the columns to import.
                The length and types should match the content of the file to
                read. If not specified, it is assumed that the entire table
                matches the file structure. Defaults to None.
            quote (str): Specifies the quoting character to be used when a data
                value is quoted. This must be a single one-byte character.
                Defaults to None, which uses the postgres default of a single
                double-quote.
            null (str): Format which nulls (or missing values) are represented.
                Defaults to None, which corresponds to an empty string.
                If a CSV file contains a row like:

                ,1,2017-05-01,25.321

                it will treat the first element as missing and inject a Null
                value into the database for the corresponding column.
        """
        LOGGER.info('Attempting to load file %s  into table %s' %
                    (filename, table_name))
        copy_query = build_copy_query('load', table_name, header=header,
                                    columns=columns, delimiter=delimiter,
                                    quote=quote, null=null)
        with open(filename, 'r') as f:
            LOGGER.info('Executing copy query\n%s' % copy_query)
            self.cursor.copy_expert(copy_query, f)
        self.conn.commit()

    def export_to_file(self, query, filename, columns=None, delimiter=',',
                header=True, null=None):
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
            null (str): Specifies the string that represents a null value.
                Defaults to None, which uses the postgres default of an
                unquoted empty string.
        """

        copy_query = build_copy_query('export',query, columns=columns,
                                            delimiter=delimiter,
                                            header=header, null=null)
        LOGGER.info('Running copy_expert with\n%s\nOutputting results to %s' %
                    (copy_query, filename))
        with open(filename, 'w') as f:
            LOGGER.info('Executing copy query\n%s' % copy_query)
            self.cursor.copy_expert(copy_query, f)

    def export_to_object(self, query, columns=None, delimiter=',', header=True,
                            null=None):
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

        copy_query = build_copy_query('export',query, columns=columns,
                                            delimiter=delimiter,
                                            header=header, null=null)
        data = None
        try:
            LOGGER.info('Running copy_expert with with\n%s\nOutputting results to '
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
