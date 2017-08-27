"""Main postgrez module
Module contains 4 core classes: Connection, Cmd, Export and Load.
"""

import psycopg2
from .utils import create_logger, read_yaml
from .exceptions import (Postgrez, PostgrezConfigError)
from .logger import create_logger
import os

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
            self.conn.close()
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
        super(Cmd, self).__init__(setup)

    def execute(self, query, query_vars=None, commit=True, columns=True):
        """Execute the supplied query.

        Args:
            query (str): Query to be executed. Query can contain placeholders,
                as long as query_vars are supplied.
            query_vars (tuple, list or dict): Variables to be executed with query.
                See http://initd.org/psycopg/docs/usage.html#query-parameters.
            many (bool): Execute multiple queries
            commit (bool): Commit any pending transaction to the database.
                Defaults to True.
            columns (bool): Return column names in results. Defaults to True.

        Returns:
            results (list): Results from query.
                Returns None if no resultset was generated (i.e. insert into
                query, update query etc..).
        Raises:
            PostgrezConnectionError: If no connection has been established.
            Exception: If an error occurs while parsing the results.
        """
        results = None
        if self._connected() == False:
            raise PostgrezConnectionError('No connection has been established')
        log.info('Executing query %s...' % query[0:QUERY_LENGTH].strip())
        self.cursor.execute(query, vars=query_vars)
        if commit:
            self.conn.commit()

        # no way to check if results were returned other than try-except
        try:
            results = self.cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            # this error is raised when there are no results to fetch
            pass

        try:
            if columns and results:
                cols = [desc[0] for desc in self.cursor.description]
                results = [{cols[i]:value for i, value in enumerate(result)}
                            for result in results]
        except Exception as e:
            raise PostgrezExecuteError('Unable to parse results from query %s'
                '... Error: %s' % (query[0:QUERY_LENGTH].strip(), e))

        return results

    ## TODO: delete? Just discovered psycopg2.cursor.execute() naturally
    ## handles executing all queries...
    def execute_many(self, query, columns=True):
        """Execute many queries. Returns the results from the executed queries.

        Args:
            query (str): Queries to be executed. Each query is separated by
                a ';'. Each query must be fully formatted with any variables.
            columns (bool): Return column names in results. Defaults to True.

        Returns:
            results (list): Results from executed queries, as a list of lists.
                Each list is the results associated with that query.
                Returns None if no results were generated.
        """
        if len(query.split(';')) == 1:
            logger.warning("Only one queried was supplied. Ensure queries are "
                "separated by ';'s")
        results = []

        for q in query.split(';'):
            query_results = self.execute(q, columns=columns)
            if query_results:
                results.append(query_results)
        return results
