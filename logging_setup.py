import logging.config
import os
from datetime import datetime


class LoggingSetup:
    _instance = None
    _log_file = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, default_path='temp.conf', default_level=logging.DEBUG, env_key='LOG_CFG'):
        if self._initialized:
            return
        self.default_path = default_path
        self.default_level = default_level
        self.env_key = env_key
        self._log_file = self.setup_logging()
        self._initialized = True

    def setup_logging(self):
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file = os.path.join(log_dir, f"log_{current_time}.log")
        self._log_file = log_file

        path = self.default_path
        value = os.getenv(self.env_key, None)
        if value:
            path = value

        if os.path.exists(path):
            # Read the configuration file
            with open(path, 'r') as file:
                config = file.read()

            # Replace the placeholder with the actual log file path
            config = config.replace('PLACEHOLDER_LOG_FILE_PATH', log_file)

            # Write the updated configuration to a temporary file
            temp_config_path = os.path.join(log_dir, 'temp_logging.conf')
            with open(temp_config_path, 'w') as file:
                file.write(config)

            # Use the updated configuration
            logging.config.fileConfig(temp_config_path)

            # Manually add the FileHandler if not already added
            root_logger = logging.getLogger()
            file_handler_exists = any(isinstance(h, logging.FileHandler) for h in root_logger.handlers)
            if not file_handler_exists:
                file_handler = logging.FileHandler(log_file)
                file_handler.setLevel(self.default_level)
                formatter = logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(message)s')
                file_handler.setFormatter(formatter)
                root_logger.addHandler(file_handler)

        else:
            logging.basicConfig(
                level=self.default_level,
                format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(log_file),
                    logging.StreamHandler()
                ]
            )

        # Verify handlers attached to the root logger
        # root_logger = logging.getLogger()
        # root_logger.debug("Logging setup complete. Logs will be written to: %s", log_file)
        # root_logger.debug("This is a debug message to test file writing.")
        # root_logger.info("This is an info message to test file writing.")
        # root_logger.error("This is an error message to test file writing.")

        # Debugging: List all handlers attached to the root logger
        # for handler in root_logger.handlers:
        #     root_logger.debug("Handler attached to root logger: %s", handler)

        return log_file

    @classmethod
    def get_log_file(cls):
        return cls._log_file
