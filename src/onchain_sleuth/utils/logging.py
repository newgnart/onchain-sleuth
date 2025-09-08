import os
import logging
import logging.handlers

logger = logging.getLogger(__name__)


def setup_logging(log_filename: str = None, level: str = "INFO"):
    """Sets up logging with console streaming and optional file logging."""
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Console handler (always present)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (optional)
    if not os.path.exists("logs"):
        os.makedirs("logs")
    if log_filename:
        file_handler = logging.handlers.RotatingFileHandler(
            f"logs/{log_filename}", maxBytes=5 * 1024 * 1024, backupCount=5
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
