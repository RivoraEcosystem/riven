"""
Logger names used throughout Riven.

Keeping names centralized prevents typos and allows users to
configure logging using standard Python logging APIs.
"""

from __future__ import annotations

# Root logger
LOGGER_NAME = "riven"

# Child loggers
ACCESS_LOGGER_NAME = f"{LOGGER_NAME}.access"
PROTOCOL_LOGGER_NAME = f"{LOGGER_NAME}.protocol"
LIFECYCLE_LOGGER_NAME = f"{LOGGER_NAME}.lifecycle"

# Default format strings
DEFAULT_LOG_FORMAT = (
    "%(levelprefix)s %(message)s"
)

DEFAULT_ACCESS_LOG_FORMAT = (
    '%(client_addr)s - "%(method)s %(path)s %(http_version)s" %(status_code)s'
)