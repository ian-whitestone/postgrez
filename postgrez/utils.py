"""
Utils module, contains utility functions used throughout the postgrez codebase.
"""

import logging
import sys
import yaml
import os
import io
import re


log = logging.getLogger(__name__)

def read_yaml(yaml_file):
    """Read a yaml file.

    Args:
        yaml_file (str): Full path of the yaml file.

    Returns:
        data (dict): Dictionary of yaml_file contents. None is returned if an
        error occurs while reading.

    Raises:
        Exception: If the yaml_file cannot be opened.
    """

    data = None
    try:
        with open(yaml_file) as f:
            # use safe_load instead load
            data = yaml.safe_load(f)
    except Exception as e:
        log.error('Unable to read file %s. Error: %s' % (yaml_file,e))

    return data


def build_copy_query(mode, query, columns=None, delimiter=',', header=True,
                        quote=None, null=None):
    """Build query used in the cursor.copy_expert() method. Refer to
    https://www.postgresql.org/docs/9.2/static/sql-copy.html for more
    information.

    Args:
        mode (str): Specifies whether the copy query is a COPY TO or a COPY FROM
            query. Accepts 'load' for copy to queries or 'export' for copy from
            queries.
        query (str): A select query or a table name
        columns (list): List of column names to export. columns should only
            be provided if you are exporting/loading a table
            (i.e. query = 'table_name'). If query is a query to export, desired
            columns should be specified in the select portion of that query
            (i.e. query = 'select col1, col2 from ...'). Defaults to None, in
            which case all columns will be exported/loaded.
        delimiter (str): Delimiter to separate columns with. Defaults to ','.
        header (boolean): Specify True to return the column names when
            exporting, or when column names are at the top of the flat file
            being loaded. Defaults to True.
        quote (str): Specifies the quoting character to be used when a data
            value is quoted. This must be a single one-byte character.
            Defaults to None, which uses the postgres default of a single
            double-quote.
        null (str): Specifies the string that represents a null value.
            Defaults to None, which uses the postgres default of an
            unquoted empty string.

    Returns:
        copy_query (str): Formatted query to run in copy_expert()

    Raises:
        Exception: If an error occurs while building hte query.
    """
    try:
        log.info('Building copy query with mode: %s' % mode)
        if columns:
            columns = '(' + ','.join(columns) + ')'

        if mode.lower() == 'load':
            copy_mode = 'FROM STDIN'
        elif mode.lower() == 'export':
            copy_mode = 'TO STDOUT'
        else:
            log.error("Mode must be 'load' or 'export'. Exiting..")
            return

        ## check if provided query is a query, or just a table name
        if re.match('\s*select', query, re.IGNORECASE):
            if columns:
                log.warning('If a query is passed in the query arg '
                            'instead of a tablename, columns must be '
                            'specified in the query itself')
                columns = None
            query = '(' + query + ')'

        copy_query = "COPY {0} {1} {2} WITH DELIMITER '{3}' " \
                        " CSV {4} {5} {6}"

        copy_query = copy_query.format(
            query,
            copy_mode,
            (columns if columns else ''),
            delimiter,
            ('HEADER' if header else ''),
            ('QUOTE ' + "'{}'".format(quote) if quote else ''),
            ('NULL ' + "'{}'".format(null) if null else '')
            )
        return copy_query
    except Exception as e:
        raise Exception('Unable to build query. Error: %s' % (e))


class IteratorFile(io.TextIOBase):
    """Given an iterator which yields strings, return a file like object for
        reading those strings. Taken from:
        https://gist.github.com/jsheedy/ed81cdf18190183b3b7d

        Attributes:
            _it: Iterator of a data object.
            _f: File-like object
    """

    def __init__(self, it):
        """
        Args:
            it (generator): Iterator of a data object. When next(it) is
                called, yields a string like 'val1|val2|val3'
        """
        self._it = it
        self._f = io.StringIO()

    def read(self, length=sys.maxsize):
        """Read file-like object.

        Args:
            length (integer): maximum file length to read to
        Returns:
            data (str): string representation of file
        """
        try:
            while self._f.tell() < length:
                row = next(self._it)
                if isinstance(row, str):
                    self._f.write(row + u"\n")
                else:
                    self._f.write(
                        unicode(row, 'utf-8').encode('utf-8') + u"\n")

        except StopIteration as e:
            # soak up StopIteration. this block is not necessary because
            # of finally, but just to be explicit
            pass

        except Exception as e:
            log.error("Error reading file-like object: %s" % e)

        finally:
            self._f.seek(0)
            data = self._f.read(length)

            # save the remainder for next read
            remainder = self._f.read()
            self._f.seek(0)
            self._f.truncate(0)
            self._f.write(remainder)
            return data
