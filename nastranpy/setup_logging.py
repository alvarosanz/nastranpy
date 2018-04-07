import logging.config


class SingleLevelFilter(logging.Filter):
    def __init__(self, passlevel, reject):
        self.passlevel = passlevel
        self.reject = reject

    def filter(self, record):
        if self.reject:
            return (record.levelno != self.passlevel)
        else:
            return (record.levelno == self.passlevel)


class LoggerWriter:
    def __init__(self, level):
        self.level = level

    def write(self, message):
        # if statement reduces the amount of newlines that are
        # printed to the logger
        if message != '\n':
            self.level(message)


config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '%(message)s'
        },
        'detailed': {
            'format': '%(asctime)s - %(levelname)s - %(message)s'
        }
    },
    'filters': {
        'info_filter': {
            '()': SingleLevelFilter,
            'passlevel': logging.INFO,
            'reject': False,
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'simple',
            'filters': ['info_filter'],
            'stream': 'ext://sys.stdout'
        },

        'model_log_file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': 'model.log',
            'mode': 'w',
            'encoding': 'utf8'
        },
    },

    'loggers': {
        '': {
            'handlers': [],
            'level': 'INFO',
            'propagate': True
        },
        'nastranpy': {
            'level': 'INFO',
            'handlers': ['console', 'model_log_file'],
            'propagate': 'no'
        }
    }
}

def setup_logging():
    logging.config.dictConfig(config)
