import logging
from logging.config import dictConfig

DEFAULT_FORMAT = (
            '%(asctime)s   %(levelname)-8s %(name)s\n'
            '%(message)s\n')


logging_config = dict(
    version = 1,
    formatters = {
        'two_line_format': {'format': DEFAULT_FORMAT}
        },
    handlers = {
        'console_handler': {'class': 'logging.StreamHandler',
              'formatter': 'two_line_format',
              'level': 'DEBUG'}
        },
    loggers = {
        'gpc': {'handlers': ['console_handler'],
                 'level': 'INFO'}
        }
)

dictConfig(logging_config)
