import sys
import logging

def create_logger(name=None, log_level='INFO',
        format='%(asctime)s  - %(module)s - %(levelname)s - %(message)s'):
    """Create a logging object.

    Args:
        name (str, optional): The name of the logger. Defaults to ``None``
        log_level (str, optional): The logging level. Defaults to ``INFO``
            Accepts: DEBUG, INFO, WARNING, ERROR, CRITICAL
        format (str, optional): Logging format. Defaults to
            ``%(asctime)s  - %(module)s - %(levelname)s - %(message)s``

    Returns:
        logging.Logger: Logger object, configured with the passed in parameters.
    """
    level_map = {
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    log_format = '%(asctime)s  - %(module)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format,
        stream=sys.stdout,
        level=level_map[log_level])
    log = logging.getLogger(name=name)
    return log
