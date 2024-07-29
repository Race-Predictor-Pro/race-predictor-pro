import logging.config
import os

def setup_logging(default_path="temp.conf", default_level=logging.DEBUG, env_key = 'LOG_CFG'):
    """SETUP LOG CONFIGURATIONS"""
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
        if os.path.exists(path):
            logging.config.fileConfig(path)
        else:
            logging.basicConfig(level=default_level)