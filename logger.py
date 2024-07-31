from logging_setup import LoggingSetup
import logging


class Logger:
    @staticmethod
    def get_logger(name='simpleLogger'):
        # Ensure the logging setup is initialized
        logging_setup = LoggingSetup()
        return logging.getLogger(name)

    @staticmethod
    def get_log_file():
        return LoggingSetup.get_log_file()

