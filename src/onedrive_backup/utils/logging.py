"""Logging configuration and utilities."""

import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Optional


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[Path] = None,
    log_to_console: bool = True,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """Setup logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        log_to_console: Whether to log to console
        max_file_size: Maximum size of log file before rotation
        backup_count: Number of backup files to keep
        
    Returns:
        Configured logger
    """
    # Create logger
    logger = logging.getLogger("onedrive_backup")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler with UTF-8 encoding support
    if log_to_console:
        import io
        import sys

        # Create a UTF-8 stream wrapper for console output
        if sys.platform == 'win32':
            # Reconfigure stdout for UTF-8 if needed
            try:
                # Try to use the buffer if it exists
                if hasattr(sys.stdout, 'buffer'):
                    utf8_stdout = io.TextIOWrapper(
                        sys.stdout.buffer,
                        encoding='utf-8',
                        errors='replace',
                        line_buffering=True
                    )
                    console_handler = logging.StreamHandler(utf8_stdout)
                else:
                    # stdout is already a TextIOWrapper, just use it
                    console_handler = logging.StreamHandler(sys.stdout)
            except Exception:
                # Fallback to standard handler
                console_handler = logging.StreamHandler()
        else:
            console_handler = logging.StreamHandler()
        
        console_handler.setLevel(getattr(logging, log_level.upper()))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Use rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)  # Always log everything to file
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(f"onedrive_backup.{name}")

class ContextualLogger:
    """Logger that adds contextual information to log messages."""
    
    def __init__(self, logger: logging.Logger, context: dict):
        """Initialize contextual logger.
        
        Args:
            logger: Base logger
            context: Context dictionary to add to messages
        """
        self.logger = logger
        self.context = context
    
    def _format_message(self, message: str) -> str:
        """Format message with context.
        
        Args:
            message: Original message
            
        Returns:
            Formatted message with context
        """
        context_str = " | ".join(f"{k}={v}" for k, v in self.context.items())
        return f"[{context_str}] {message}"
    
    def debug(self, message: str):
        """Log debug message with context."""
        self.logger.debug(self._format_message(message))
    
    def info(self, message: str):
        """Log info message with context."""
        self.logger.info(self._format_message(message))
    
    def warning(self, message: str):
        """Log warning message with context."""
        self.logger.warning(self._format_message(message))
    
    def error(self, message: str):
        """Log error message with context."""
        self.logger.error(self._format_message(message))
    
    def critical(self, message: str):
        """Log critical message with context."""
        self.logger.critical(self._format_message(message))

class TimedOperation:
    """Context manager for timing operations and logging results."""
    
    def __init__(self, logger: logging.Logger, operation_name: str, log_level: str = "INFO"):
        """Initialize timed operation.
        
        Args:
            logger: Logger to use
            operation_name: Name of the operation
            log_level: Log level for timing messages
        """
        self.logger = logger
        self.operation_name = operation_name
        self.log_level = getattr(logging, log_level.upper())
        self.start_time = None
    
    def __enter__(self):
        """Start timing."""
        self.start_time = datetime.now()
        self.logger.log(self.log_level, f"Starting {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timing and log results."""
        if self.start_time:
            duration = (datetime.now() - self.start_time).total_seconds()
            if exc_type is None:
                self.logger.log(self.log_level, f"Completed {self.operation_name} in {duration:.2f}s")
            else:
                self.logger.error(f"Failed {self.operation_name} after {duration:.2f}s: {exc_val}")
