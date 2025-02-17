import logging
import os
from logging.handlers import TimedRotatingFileHandler


class LoggerManager:

    def __init__(
        self,
        log_dir: str = "logs",
        backup_count: int = 7,
        rotation_when: str = "D",
        rotation_interval: int = 1,
        default_log_level=logging.DEBUG,
    ):

        self.log_dir = log_dir
        self.backup_count = backup_count
        self.rotation_when = rotation_when
        self.rotation_interval = rotation_interval
        self.default_log_level = default_log_level

        self.log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

        self._global_logger = None

        self._setup_global_logger()

    def _setup_global_logger(self):

        self._global_logger = logging.getLogger("global")
        self._global_logger.setLevel(self.default_log_level)

        global_handler = TimedRotatingFileHandler(
            os.path.join(self.log_dir, "global.log"),
            when=self.rotation_when,
            interval=self.rotation_interval,
            backupCount=self.backup_count,
            encoding="utf-8",
        )
        global_handler.setFormatter(logging.Formatter(self.log_format))

        if not self._global_logger.handlers:
            self._global_logger.addHandler(global_handler)

    def get_logger(self, name: str, level=None) -> logging.Logger:

        if level is None:
            level = self.default_log_level

        logger = logging.getLogger(name)
        logger.setLevel(level)

        if not logger.handlers:
            file_handler = TimedRotatingFileHandler(
                os.path.join(self.log_dir, f"{name}.log"),
                when=self.rotation_when,
                interval=self.rotation_interval,
                backupCount=self.backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(logging.Formatter(self.log_format))
            logger.addHandler(file_handler)

            logger.propagate = True

        return logger

    @property
    def global_logger(self) -> logging.Logger:
        return self._global_logger
