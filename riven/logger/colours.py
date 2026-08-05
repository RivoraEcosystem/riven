"""
ANSI color utilities used by Riven's logging system.

This module is intentionally lightweight and independent from the rest of
the logging implementation.
"""

from __future__ import annotations

import logging
import sys
from enum import StrEnum

__all__ = (
    "ANSI",
    "supports_color",
    "colorize",
    "level_color",
    "status_color",
    "method_color",
)


class ANSI(StrEnum):
    RESET = "\033[0m"

    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"


def supports_color(stream=None) -> bool:
    """
    Return True if ANSI colors should be emitted.

    Colors are disabled automatically when output is redirected to a file.
    """

    if stream is None:
        stream = sys.stderr

    return hasattr(stream, "isatty") and stream.isatty()


def colorize(text: str, color: ANSI, enabled: bool) -> str:
    """Wrap text with ANSI escape sequences."""

    if not enabled:
        return text

    return f"{color}{text}{ANSI.RESET}"


_LEVEL_COLORS = {
    logging.DEBUG: ANSI.BRIGHT_BLACK,
    logging.INFO: ANSI.GREEN,
    logging.WARNING: ANSI.YELLOW,
    logging.ERROR: ANSI.RED,
    logging.CRITICAL: ANSI.BRIGHT_RED,
}


_METHOD_COLORS = {
    "GET": ANSI.GREEN,
    "POST": ANSI.BLUE,
    "PUT": ANSI.YELLOW,
    "PATCH": ANSI.MAGENTA,
    "DELETE": ANSI.RED,
    "HEAD": ANSI.CYAN,
    "OPTIONS": ANSI.BRIGHT_CYAN,
}


def level_color(level: int) -> ANSI:
    """Return the ANSI color for a logging level."""
    return _LEVEL_COLORS.get(level, ANSI.WHITE)


def method_color(method: str) -> ANSI:
    """Return the ANSI color for an HTTP method."""
    return _METHOD_COLORS.get(method.upper(), ANSI.WHITE)


def status_color(status: int) -> ANSI:
    """Return the ANSI color for an HTTP status code."""

    if 100 <= status < 200:
        return ANSI.BRIGHT_CYAN

    if 200 <= status < 300:
        return ANSI.GREEN

    if 300 <= status < 400:
        return ANSI.CYAN

    if 400 <= status < 500:
        return ANSI.YELLOW

    return ANSI.RED