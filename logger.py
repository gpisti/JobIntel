import logging
import os
from logging.handlers import TimedRotatingFileHandler


class LoggerManager:
    """
    Manages loggers with configurable time-based file rotation, backup control, and
    a default log level. Provides named loggers, a global logger, and dedicated
    logging for failed database insert operations.
    """

    def __init__(
        self,
        log_dir: str = "logs",
        backup_count: int = 7,
        rotation_when: str = "D",
        rotation_interval: int = 1,
        default_log_level=logging.DEBUG,
    ):
        """
        Initialize a logger with a specified directory, rotation settings, and default log level.

        Args:
            log_dir (str): Directory where log files are stored.
            backup_count (int): Maximum number of backup files to keep.
            rotation_when (str): Time unit for log rotation (e.g., 'D' for days).
            rotation_interval (int): Frequency of rotation based on the time unit.
            default_log_level (int): Default logging level.
        """
        self.log_dir = log_dir
        self.backup_count = backup_count
        self.rotation_when = rotation_when
        self.rotation_interval = rotation_interval
        self.default_log_level = default_log_level
        self.log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

        self._loggers = {}
        
        self._global_logger = self.get_logger("global")

    def get_logger(self, name: str, level=None) -> logging.Logger:
        """
        Retrieve a named logger at a specified log level, creating a new file handler if necessary.
        If no level is provided, the default log level is used. The returned logger does not
        propagate log messages to its parent.
        
        Args:
            name (str): Name of the logger, also used for the log file
            level (int, optional): Logging level. Defaults to None (uses default_log_level).
            
        Returns:
            logging.Logger: Configured logger instance
        """
        if name in self._loggers:
            return self._loggers[name]
            
        if level is None:
            level = self.default_log_level

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = False

        if not logger.handlers:
            log_path = os.path.join(self.log_dir, f"{name}.log")
            file_handler = TimedRotatingFileHandler(
                log_path,
                when=self.rotation_when,
                interval=self.rotation_interval,
                backupCount=self.backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(logging.Formatter(self.log_format))
            logger.addHandler(file_handler)

        self._loggers[name] = logger
        return logger

    @property
    def global_logger(self) -> logging.Logger:
        """
        Returns the global logger used for centralized logging.

        Returns:
            logging.Logger: The global logger instance.
        """
        return self._global_logger

    def log_milestone(self, message: str, component: str = None):
        """
        Logs an important application milestone to the global logger.
        
        Args:
            message (str): The milestone message to log
            component (str, optional): The system component generating this milestone
        """
        if component:
            self._global_logger.info(f"[{component.upper()}] {message}")
        else:
            self._global_logger.info(message)

    def log_failed_insert(self, table: str, data: dict, error: str):
        """
        Logs details of a failed database insert to a dedicated log file and the global logger.

        Parameters:
            table (str): Name of the database table.
            data (dict): Data that failed to be inserted.
            error (str): Description of the encountered error.
        """
        failed_log_path = os.path.join(self.log_dir, "failed_inserts.log")
        with open(failed_log_path, "a", encoding="utf-8") as f:
            f.write(f"Table: {table}, Data: {data}, Error: {error}\n")
            
        self._global_logger.error(f"Failed insert in {table}: {error} - Data: {data}")

    def log_progress(self, current: int, total: int, operation: str = "processing", component: str = None):
        """
        Log standardized progress information with milestone tracking.
        
        Args:
            current: Current number of items processed
            total: Total number of items to process
            operation: Description of the operation for log messages
            component: The component generating the progress update
        """
        if total <= 0:
            return
            
        progress = (current / total) * 100
        
        if component:
            logger = self.get_logger(component)
        else:
            logger = self._global_logger
            
        logger.info(f"Progress: {progress:.1f}% complete ({current}/{total})")
        
        milestones = [10, 25, 50, 75, 100]
        for milestone in milestones:
            if (progress >= milestone and 
                ((current - 1) / total * 100) < milestone):
                self.log_milestone(
                    f"{milestone}% complete: {current}/{total} items {operation}",
                    component
                )
                
