import importlib
import os
import os.path as op
from logging import config

LOG_DIR = op.abspath(op.join(op.dirname(__file__), 'logs/'))

ENVIRONMENT_VARIABLE = 'ETL_SETTINGS'
ENV = os.environ.get(ENVIRONMENT_VARIABLE, 'configuration.development')
SLACK_API_KEY = 'xoxp-2916621745-11543183236-57556260833-46971c2fff'

app_settings = importlib.import_module(ENV)

handlers = ['console',
            'execution_log',
            'warning_log',
            'error_log',
            'slack_error',
            'slack_info'] if ENV in [
                'configuration.staging',
                'configuration.production'] else ['console',
                                                  'execution_log',
                                                  'warning_log',
                                                  'error_log']

LOGGING = {
    'version': 1,
    'formatters': {
        'timed_log': {
            'format': '%(levelname)s %(asctime)s: %(name)s: %(funcName)s: %(lineno)d: %(message)s: %(args)s',
        },
        'slack': {
            'format': '*%(levelname)s* %(asctime)s: %(name)s: %(funcName)s: %(lineno)d: %(message)s: %(args)s',
        },
        'simple': {
            'format': '%(levelname)s: %(name)s: %(message)s: %(args)s',
        }
    },
    'filters': {},
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
        'execution_log': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'utc': True,
            'formatter': 'timed_log',
            'backupCount': 30,  # Store for a month
            'filename': op.join(LOG_DIR, 'exec'),
        },
        'warning_log': {
            'level': 'WARNING',
            'class': 'logging.handlers.RotatingFileHandler',
            'backupCount': 10,  # Keep 10MB worth of errors, should be fine
            'formatter': 'timed_log',
            'filename': op.join(LOG_DIR, 'warning'),
        },
        'error_log': {
            'level': 'ERROR',
            'class': 'logging.handlers.RotatingFileHandler',
            'backupCount': 10,  # Keep 10MB worth of errors, should be fine
            'formatter': 'timed_log',
            'filename': op.join(LOG_DIR, 'error'),
        },
        # 'slack_error': {
        #     'level': 'ERROR',
        #     'api_key': SLACK_API_KEY,
        #     'class': 'app.utility.slacker_log_handler.SlackerLogHandler',
        #     'channel': '#bi_logger'
        # },
        # 'slack_info': {
        #     'level': 'INFO',
        #     'api_key': SLACK_API_KEY,
        #     'class': 'app.utility.slacker_log_handler.SlackerLogHandler',
        #     'channel': '#bi_logger'
        # },
    },
    'root': {
        'handlers': handlers,
        'level': 'INFO',
        'propagate': False,
    }
}
config.dictConfig(LOGGING)
