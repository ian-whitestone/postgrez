"""Utility functions used throughout the postgrez codebase.
"""

from .logger import create_logger
import sys
import yaml
import os
import io


log = create_logger(__name__)

def read_yaml(yaml_file):
    """Read a yaml file.
    Args:
        yaml_file (str): Full path of the yaml file.

    Returns:
        data (dict): Dictionary of yaml_file contents.
            None is returned if an error occurs while reading.

    Raises:
        Exception: If the yaml_file cannot be opened
    """
    data = None
    try:
        with open(yaml_file) as f:
            # use safe_load instead load
            data = yaml.safe_load(f)
    except Exception as e:
        log.error('Unable to read file %s. Error: %s' % (yaml_file,e))

    return data



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
