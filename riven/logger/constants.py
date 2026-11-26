"""
Constants used by Riven's logging system.
"""

from __future__ import annotations

import logging

__all__ = (
    "LOGGER_NAME",
    "ACCESS_LOGGER_NAME",
    "PROTOCOL_LOGGER_NAME",
    "LIFECYCLE_LOGGER_NAME",
    "NETWORK_LOGGER_NAME",
    "NETWORK_LOG_LEVEL",
    "DEFAULT_LOG_FORMAT",
    "DEFAULT_ACCESS_LOG_FORMAT",
)

# Logger names
LOGGER_NAME = "riven"
ACCESS_LOGGER_NAME = f"{LOGGER_NAME}.access"
PROTOCOL_LOGGER_NAME = f"{LOGGER_NAME}.protocol"
LIFECYCLE_LOGGER_NAME = f"{LOGGER_NAME}.lifecycle"
NETWORK_LOGGER_NAME = f"{LOGGER_NAME}.network"
NETWORK_LOG_LEVEL = 25
logging.addLevelName(NETWORK_LOG_LEVEL, "NETWORK")

# Formatter templates
DEFAULT_LOG_FORMAT = "%(levelprefix)s %(message)s"

DEFAULT_ACCESS_LOG_FORMAT = '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s'
