import logging
from logging.config import dictConfig
import gpc

logging_config = gpc.config.load()['logging']

if logging_config is not None:
    try:
        dictConfig(logging_config)
    except Exception as e:
        msg = 'Could not initialize logging with current logging config.'
        raise gpc.GenericError(msg) from e
