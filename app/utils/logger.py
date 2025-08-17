import os
import logging
import logging.config
import json
from datetime import datetime
import sys
import traceback
from typing import Dict, Any, Optional, Union

# Default logging level
DEFAULT_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Log format for different environments
LOG_FORMAT = os.getenv("LOG_FORMAT", "json" if os.getenv("ENVIRONMENT", "").lower() in ["production", "staging"] else "text")

class JsonFormatter(logging.Formatter):
    """
    Custom formatter that outputs log records as JSON objects.
    This is particularly useful in production environments where logs are collected
    by systems like ELK, Datadog, or CloudWatch.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON string."""
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "process": record.process,
            "thread": record.thread,
        }
        
        # Add exception info if available
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info),
            }
        
        # Add extra fields if available
        if hasattr(record, "extra") and record.extra:
            log_data.update(record.extra)
        
        return json.dumps(log_data)

def setup_logging(
    log_level: str = DEFAULT_LOG_LEVEL,
    log_format: str = LOG_FORMAT,
    log_file: Optional[str] = None,
) -> None:
    """
    Configure the logging system for the application.
    
    Args:
        log_level: The minimum log level to record (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: The format of the logs (json or text)
        log_file: Path to the log file, if None logs will be sent to stdout
    """
    # Define logging configuration
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": JsonFormatter,
            },
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "detailed": {
                "format": "%(asctime)s [%(levelname)s] %(name)s (%(module)s:%(lineno)d): %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "json" if log_format == "json" else "standard",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "": {  # Root logger
                "handlers": ["console"],
                "level": log_level,
                "propagate": True,
            },
            "uvicorn": {
                "handlers": ["console"],
                "level": log_level,
                "propagate": False,
            },
            "sqlalchemy.engine": {
                "handlers": ["console"],
                "level": "WARNING",  # Reduce SQL query logging
                "propagate": False,
            },
            "celery": {
                "handlers": ["console"],
                "level": log_level,
                "propagate": False,
            },
        },
    }
    
    # Add file handler if log_file is provided
    if log_file:
        log_config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": log_level,
            "formatter": "json" if log_format == "json" else "detailed",
            "filename": log_file,
            "maxBytes": 10485760,  # 10 MB
            "backupCount": 5,
            "encoding": "utf8",
        }
        log_config["loggers"][""]["handlers"].append("file")
    
    # Apply configuration
    logging.config.dictConfig(log_config)
    
    # Log startup message
    logger = logging.getLogger("app.logging")
    logger.info(
        f"Logging configured with level={log_level}, format={log_format}, "
        f"file={'enabled' if log_file else 'disabled'}"
    )

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: The name of the logger, typically the module name
        
    Returns:
        A configured logger instance
    """
    return logging.getLogger(name)

class LoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that adds context to log messages.
    
    Usage:
        logger = LoggerAdapter(get_logger(__name__), {"request_id": "123"})
        logger.info("Processing request")  # Will include request_id in the log
    """
    
    def __init__(self, logger: logging.Logger, extra: Dict[str, Any] = None):
        """Initialize the adapter with a logger and extra context."""
        super().__init__(logger, extra or {})
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """Process the log message to add context."""
        # Ensure 'extra' exists in kwargs
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        
        # Update 'extra' with our extra context
        kwargs['extra'].update(self.extra)
        
        return msg, kwargs
    
    def bind(self, **kwargs) -> 'LoggerAdapter':
        """
        Create a new logger adapter with additional context.
        
        Args:
            **kwargs: Additional context to add to the logger
            
        Returns:
            A new LoggerAdapter with the combined context
        """
        new_extra = self.extra.copy()
        new_extra.update(kwargs)
        return LoggerAdapter(self.logger, new_extra)