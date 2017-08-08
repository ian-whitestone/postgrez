"""Main psyco-wrapper module
Module contains X core classes: Connection, Cmd, Export and Load.
"""

import psycopg2
from utils import create_logger

log = logger(__name__)

class Connection(object):
    """
    Class which establishes connections to a PostgresSQL (or Postgres based)
    database.

    Methods used internally by the class are prefixed with a `_`.

    Parameters
        config (str): Name of the database in ~/.moosez_yaml

    Attributes
        host (str): Database host address
        port (int): Connection port number (defaults to 5432 if not provided)
        database (str): Name of the database
        user (str): Username used to authenticate
        password (str): Password used to authenticate
        conn (psycopg2 connection): psycopg2 connection object
        cursor (psycopg2 cursor): psycopg2 cursor object, associated with the connection object
    """
    def __init__(self, config):
        self.config = config
        self.host = None
        self.port = 5432
        self.database = None
        self.user = None
        self.password = None
        ## Fetch attributes from file
        self._get_attributes()
        self._connect()

    def _get_attributes():
        """Read database connection parameters from ~/.moosez_yaml.
        """

        return

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
        """Close the cursor and connection objects.
        This code is automatically executed after the with statement is
        completed or if any error arises during the process.
        Ref: https://stackoverflow.com/questions/1984325/explaining-pythons-enter-and-exit
        """
        log.info("Attempting to close connection ...")
        self._disconnect()
        log.info("Connection closed")
