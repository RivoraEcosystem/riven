"""
Default logging configuration for Riven.

This configuration is used when no custom ``log_config`` is supplied.
It is compatible with ``logging.config.dictConfig``.
"""

from __future__ import annotations

from typing import Any

from .constants import (
    LOGGER_NAME,
    ACCESS_LOGGER_NAME,
    PROTOCOL_LOGGER_NAME,
    LIFECYCLE_LOGGER_NAME,
    DEFAULT_LOG_FORMAT,
    DEFAULT_ACCESS_LOG_FORMAT,
)

LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "()": "riven.logger.formatter.DefaultFormatter",
            "fmt": DEFAULT_LOG_FORMAT,
            "use_colors": None,
        },
        "access": {
            "()": "riven.logger.formatter.AccessFormatter",
            "fmt": DEFAULT_ACCESS_LOG_FORMAT,
            "use_colors": None,
        },
    },
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": "ext://sys.stderr",
        },
        "access": {
            "class": "logging.StreamHandler",
            "formatter": "access",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        LOGGER_NAME: {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        ACCESS_LOGGER_NAME: {
            "handlers": ["access"],
            "level": "INFO",
            "propagate": False,
        },
        PROTOCOL_LOGGER_NAME: {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        LIFECYCLE_LOGGER_NAME: {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
    },
}