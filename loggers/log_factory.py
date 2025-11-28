import asyncio
import os
import json
import logging
import threading
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from functools import wraps
from contextlib import contextmanager

class ColoredConsoleFormatter(logging.Formatter):
    """
    Formatter that adds colors to console output based on log levels.
    """
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[32m',  # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',  # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, '')
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)


class IndentedFormatter(logging.Formatter):
    """
    Custom logging formatter to place the log message on a new line and indent it.
    """

    def __init__(self, fmt=None, datefmt=None, style='%', validate=True, indent_size=25):
        super().__init__(fmt, datefmt, style, validate)
        self.indent_size = indent_size

    def format(self, record):
        # Process the log message using the base formatter
        original_message = super().format(record)

        # Add a newline and indent the log message part
        if ": " in original_message:
            # Split the log parts: 'timestamp [level]:' and 'message'
            parts = original_message.split(": ", 1)
            indent = " " * self.indent_size
            formatted_message = f"{parts[0]}:\n{indent}{parts[1]}"
        else:
            formatted_message = original_message  # Fallback if format is unexpected

        return formatted_message


class JSONFormatter(logging.Formatter):
    """
    Formatter that outputs log records as JSON objects.
    """

    def format(self, record):
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'thread': record.thread,
            'thread_name': record.threadName,
        }

        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)

        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename',
                           'module', 'exc_info', 'exc_text', 'stack_info', 'lineno', 'funcName',
                           'created', 'msecs', 'relativeCreated', 'thread', 'threadName',
                           'processName', 'process', 'message']:
                log_entry[key] = value

        return json.dumps(log_entry)


class LogFilter:
    """
    Custom log filter to include/exclude messages based on criteria.
    """

    def __init__(self, include_patterns: List[str] = None, exclude_patterns: List[str] = None):
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []

    def filter(self, record):
        message = record.getMessage()

        # If include patterns are specified, message must match at least one
        if self.include_patterns:
            if not any(pattern in message for pattern in self.include_patterns):
                return False

        # If exclude patterns are specified, message must not match any
        if self.exclude_patterns:
            if any(pattern in message for pattern in self.exclude_patterns):
                return False

        return True


class PerformanceLogger:
    """
    Context manager and decorator for measuring execution time.
    """

    def __init__(self, logger: logging.Logger, operation_name: str):
        self.logger = logger
        self.operation_name = operation_name
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.debug(f"Starting operation: {self.operation_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        self.logger.info(f"Operation '{self.operation_name}' completed in {duration:.4f}s")


class LoggerManager:
    """
    Centralized logger management system.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.loggers: Dict[str, logging.Logger] = {}
            self.config: Dict[str, Any] = {}
            self.log_hooks: List[Callable] = []
            # Guard flag retained for compatibility with HookHandler attachment logic
            self._root_hook_attached: bool = False
            self.initialized = True

    def add_hook(self, hook_func: Callable[[logging.LogRecord], None]):
        """Add a hook function that will be called for every log record."""
        self.log_hooks.append(hook_func)

    def remove_hook(self, hook_func: Callable[[logging.LogRecord], None]):
        """Remove a previously added hook function."""
        if hook_func in self.log_hooks:
            self.log_hooks.remove(hook_func)

    def get_all_loggers(self) -> Dict[str, logging.Logger]:
        """Get all managed loggers."""
        return self.loggers.copy()

    def set_global_level(self, level: int):
        """Set logging level for all managed loggers."""
        for logger in self.loggers.values():
            logger.setLevel(level)

    def cleanup_old_logs(self, log_dir: str = "logs", days_to_keep: int = 30):
        """Remove log files older than specified days."""
        if not os.path.exists(log_dir):
            return

        cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)

        for filename in os.listdir(log_dir):
            filepath = os.path.join(log_dir, filename)
            if os.path.isfile(filepath) and filename.endswith('.log'):
                if os.path.getmtime(filepath) < cutoff_time:
                    try:
                        os.remove(filepath)
                        print(f"Removed old log file: {filename}")
                    except OSError as e:
                        print(f"Failed to remove {filename}: {e}")


class HookHandler(logging.Handler):
    """
    Custom handler that triggers registered hooks.
    """

    def __init__(self, hooks: List[Callable]):
        super().__init__()
        self.hooks = hooks

    def emit(self, record):
        # Prevent duplicate forwarding across multiple HookHandlers
        if getattr(record, "_hook_forwarded", False):
            return
        for hook in self.hooks:
            try:
                hook(record)
                setattr(record, "_hook_forwarded", True)
            except Exception:
                pass  # Don't let hook failures break logging


def log_performance(operation_name: str = None):
    """
    Decorator to log function execution time for both sync and async functions.
    """

    def decorator(func):
        if asyncio.iscoroutinefunction(func):
            # Handle async functions
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                logger = logging.getLogger(func.__module__)
                op_name = operation_name or f"{func.__name__}"

                with PerformanceLogger(logger, op_name):
                    return await func(*args, **kwargs)

            return async_wrapper
        else:
            # Handle sync functions
            @wraps(func)
            def wrapper(*args, **kwargs):
                logger = logging.getLogger(func.__module__)
                op_name = operation_name or f"{func.__name__}"

                with PerformanceLogger(logger, op_name):
                    return func(*args, **kwargs)

            return wrapper

    return decorator


@contextmanager
def log_context(logger: logging.Logger, operation_name: str, level: int = logging.INFO):
    """
    Context manager for logging operation start and end.
    """
    logger.log(level, f"Starting: {operation_name}")
    start_time = datetime.now()

    try:
        yield
        duration = (datetime.now() - start_time).total_seconds()
        logger.log(level, f"Completed: {operation_name} (took {duration:.4f}s)")
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Failed: {operation_name} after {duration:.4f}s - {str(e)}")
        raise
