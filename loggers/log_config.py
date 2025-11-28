import asyncio
import logging
import os
from logging.handlers import TimedRotatingFileHandler

from dotenv import load_dotenv

from loggers.error_reporter import ErrorReporter, ReportingHandler
from loggers.log_factory import ColoredConsoleFormatter, IndentedFormatter

def setup_logging(app_name: str, default_level=logging.INFO):
    """
    Initializes the application's logging system.

    The log level can be set via the LOG_LEVEL environment variable.
    This function configures the root logger to send logs to the console,
    a rotating file, and the email error reporter. All loggers in the
    application will inherit this configuration.

    Args:
        app_name: The name of the application, used for the log file.
        default_level: The fallback minimum level of logs to process if LOG_LEVEL env var is not set.
    """
    load_dotenv()
    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")

    # Get log level from environment variable, with a fallback
    log_level_name = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_level = getattr(logging, log_level_name, default_level)

    # 1. Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 2. Clear any existing handlers to prevent duplicate logs
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # 3. Create a console handler with colorized output
    console_handler = logging.StreamHandler()
    console_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    console_handler.setFormatter(ColoredConsoleFormatter(console_format))
    root_logger.addHandler(console_handler)

    # 4. Create a rotating file handler
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{app_name}.log")
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        backupCount=5,
        encoding="utf-8"
    )
    file_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    file_handler.setFormatter(IndentedFormatter(file_format))
    root_logger.addHandler(file_handler)

    # 5. Create the email error reporter and its handler
    error_reporter = None
    if email and password:
        error_reporter = ErrorReporter(email=email, app_password=password)
        reporting_handler = ReportingHandler(notifier=error_reporter)
        reporting_handler.setLevel(logging.ERROR)  # Only send ERROR and CRITICAL to email
        root_logger.addHandler(reporting_handler)
        logging.info("Error reporter initialized and handler added.")
    else:
        logging.warning("Email or password not found in environment variables. Email reporting is disabled.")

    logging.info(f"Logging initialized with level {log_level_name}")
    return error_reporter
