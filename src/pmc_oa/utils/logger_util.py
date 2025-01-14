import logging
from pathlib import Path


class LoggerSetup:
    def __init__(self, name='logger', log_dir='../logs', log_filename='download.log'):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level=logging.DEBUG)
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Construct the full path for the log file
        log_file = self.log_dir / log_filename

        # Create handlers
        console_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(log_file, mode='w')

        # Set levels
        console_handler.setLevel(logging.INFO)
        file_handler.setLevel(logging.DEBUG)

        # Create formatters and add to handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger