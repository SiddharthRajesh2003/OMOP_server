import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict, Any

class SafeStreamHandler(logging.StreamHandler):
    """
    A stream handler that catches UnicodeEncodeError and writes those messages to a separate error log file.
    """
    def __init__(self, error_log_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.error_log_path = error_log_path

    def emit(self, record):
        try:
            super().emit(record)
        except UnicodeEncodeError as e:
            # Write the error message to a separate error log file
            with open(self.error_log_path, 'a', encoding='utf-8') as f:
                f.write(self.format(record) + '\n')

class SafeFileHandler(RotatingFileHandler):
    """
    A file handler that catches errors and writes them to a separate error log file.
    """
    def __init__(self, error_log_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.error_log_path = error_log_path

    def emit(self, record):
        try:
            super().emit(record)
        except Exception as e:
            # Write the error to a separate error log file
            try:
                with open(self.error_log_path, 'a', encoding='utf-8') as f:
                    f.write(f"Logging error occurred: {str(e)}\n")
                    f.write(f"Original record: {record.getMessage()}\n")
                    f.write("---\n")
            except:
                # If we can't even write to the error log, there's not much we can do
                pass

class LoggingManager:
    """
    A reusable logging manager that can be imported and used across multiple modules.
    Provides consistent logging configuration with both file and console output.
    Automatically configures logging when first imported.
    """
    
    _instance = None
    _configured = False
    
    def __new__(cls):
        """Singleton pattern to ensure only one logging manager exists."""
        if cls._instance is None:
            cls._instance = super(LoggingManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the logging manager."""
        if not hasattr(self, 'initialized'):
            self.log_dir = None
            self.log_file = None
            self.error_log_file = None
            self.formatter = None
            self.loggers: Dict[str, logging.Logger] = {}
            self.initialized = True
            
            # Auto-configure logging with default settings
            self._auto_configure()
    
    def _auto_configure(self):
        """
        Automatically configure logging with default settings.
        This is called when the LoggingManager is first instantiated.
        """
        if not LoggingManager._configured:
            # Get the caller's directory for log file placement
            import inspect
            frame = inspect.currentframe()
            try:
                # Go up the call stack to find the real caller (skip __init__ and __new__)
                caller_frame = frame.f_back.f_back if frame.f_back else frame
                if caller_frame and '__file__' in caller_frame.f_globals:
                    caller_file = caller_frame.f_globals['__file__']
                    caller_dir = os.path.dirname(os.path.abspath(caller_file))
                    log_dir = os.path.join(caller_dir, 'logs')
                else:
                    # Fallback to current working directory
                    log_dir = os.path.join(os.getcwd(), 'logs')
            finally:
                del frame
            
            omop_server_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            log_dir = os.path.join(omop_server_dir, 'logs')
            self.setup_logging(
                log_dir=log_dir,
                log_filename='application.log',
                log_level=logging.INFO,
                console_output=True,
                file_output=True
            )
    
    def setup_logging(self, 
                     log_dir: Optional[str] = None,
                     log_filename: str = 'application.log',
                     log_level: int = logging.INFO,
                     format_string: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     date_format: str = '%Y-%m-%d %H:%M:%S',
                     console_output: bool = True,
                     file_output: bool = True,
                     max_bytes: int = 10*1024*1024,  # 10MB
                     backup_count: int = 5) -> None:
        """
        Setup logging configuration.
        
        Args:
            log_dir: Directory for log files. If None, creates 'logs' dir relative to caller
            log_filename: Name of the log file
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format_string: Format string for log messages
            date_format: Date format for timestamps
            console_output: Whether to output to console
            file_output: Whether to output to file
            max_bytes: Maximum size for log file before rotation
            backup_count: Number of backup files to keep
        """
        # Set up log directory
        if log_dir is None:
            omop_server_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.log_dir = os.path.join(omop_server_dir, 'logs')
        else:
            self.log_dir = log_dir
        
        # Create log directory if it doesn't exist
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Set up log file paths
        self.log_file = os.path.join(self.log_dir, log_filename)
        self.error_log_file = os.path.join(self.log_dir, 'log_error.log')
        
        # Create formatter
        self.formatter = logging.Formatter(format_string, date_format)
        
        # Store configuration
        self.config = {
            'log_level': log_level,
            'console_output': console_output,
            'file_output': file_output,
            'max_bytes': max_bytes,
            'backup_count': backup_count
        }
        
        # Configure root logger to prevent duplicate messages
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # Clear any existing handlers to avoid duplicates
        root_logger.handlers.clear()
        
        # Suppress default logging error output to stderr
        logging.raiseExceptions = False
        
        # Set up handlers
        if file_output:
            self._setup_file_handler()
        
        if console_output:
            self._setup_console_handler()
        
        LoggingManager._configured = True
        
        # Log setup completion
        logger = self.get_logger(__name__)
        logger.info(f"Logging setup completed. Log file: {self.log_file}")
        logger.info(f"Error log file: {self.error_log_file}")
    
    def _setup_file_handler(self) -> None:
        """Set up rotating file handler."""
        
        file_handler = SafeFileHandler(
            self.error_log_file,
            self.log_file,
            maxBytes=self.config['max_bytes'],
            backupCount=self.config['backup_count'],
            mode='a'
        )
        file_handler.setLevel(self.config['log_level'])
        file_handler.setFormatter(self.formatter)
        
        # Add to root logger
        logging.getLogger().addHandler(file_handler)
    
    def _setup_console_handler(self) -> None:
        """Set up console handler."""
        console_handler = SafeStreamHandler(self.error_log_file)
        console_handler.setLevel(self.config['log_level'])
        console_handler.setFormatter(self.formatter)
        logging.getLogger().addHandler(console_handler)

    
    def get_logger(self, name: str) -> logging.Logger:
        """
        Get a logger instance for the given name.
        
        Args:
            name: Name for the logger (usually __name__)
            
        Returns:
            logging.Logger: Configured logger instance
        """
        # No need to check if configured - auto-configuration happens in __init__
        if name not in self.loggers:
            logger = logging.getLogger(name)
            logger.setLevel(self.config['log_level'])
            # Don't add handlers here - they're on the root logger
            self.loggers[name] = logger
        
        return self.loggers[name]
    
    def set_level(self, level: int) -> None:
        """
        Change the logging level for all loggers.
        
        Args:
            level: New logging level
        """
        logging.getLogger().setLevel(level)
        for handler in logging.getLogger().handlers:
            handler.setLevel(level)
        
        # Update stored config
        self.config['log_level'] = level
    
    def add_file_handler(self, filename: str, level: int = None) -> None:
        """
        Add an additional file handler.
        
        Args:
            filename: Name of the additional log file
            level: Logging level for this handler (uses default if None)
        """
        if level is None:
            level = self.config['log_level']
        
        additional_file = os.path.join(self.log_dir, filename)
        file_handler = SafeFileHandler(self.error_log_file, additional_file, mode='a')
        file_handler.setLevel(level)
        file_handler.setFormatter(self.formatter)
        
        logging.getLogger().addHandler(file_handler)
    
    def get_log_file_path(self) -> str:
        """Get the current log file path."""
        return self.log_file
    
    def get_log_dir(self) -> str:
        """Get the current log directory."""
        return self.log_dir
    
    def get_error_log_file_path(self) -> str:
        """Get the current error log file path."""
        return self.error_log_file


# Create a global instance that auto-configures on import
_global_manager = LoggingManager()


# Convenience function for easy import
def setup_logging(**kwargs) -> LoggingManager:
    """
    Convenience function to reconfigure logging with custom settings.
    
    Args:
        **kwargs: Arguments to pass to LoggingManager.setup_logging()
        
    Returns:
        LoggingManager: The logging manager instance
    """
    _global_manager.setup_logging(**kwargs)
    return _global_manager


def get_logger(name: str) -> logging.Logger:
    """
    Convenience function to get a logger.
    
    Args:
        name: Name for the logger (usually __name__)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    return _global_manager.get_logger(name)