"""Main postgrez module
Module contains 4 core classes: Connection, Cmd, Export and Load.
"""

import psycopg2
from .utils import create_logger, read_yaml
from .exceptions import (Postgrez, PostgrezConfigError)
from .logger import create_logger
import os

log = create_logger(__name__)

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
            setup (str): Name of the setup to use in ~/.postgrez
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
        self._validate_attributes()
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

        Raises
        Exception
            If there is a problem creating a connection.
        """
        try:
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

        Raises
        Exception
            If there is a problem creating a connection.
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
