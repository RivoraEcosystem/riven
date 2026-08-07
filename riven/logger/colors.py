"""
ANSI colour utilities for Riven logging.
"""

from __future__ import annotations

import logging
import sys
from enum import StrEnum
from typing import TextIO

__all__ = (
    "ANSIColor",
    "supports_color",
    "colorize",
    "level_color",
    "method_color",
    "status_color",
)


class ANSIColor(StrEnum):
    """ANSI escape sequences."""

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


_LEVEL_COLORS: dict[int, ANSIColor] = {
    logging.DEBUG: ANSIColor.BRIGHT_BLACK,
    logging.INFO: ANSIColor.GREEN,
    logging.WARNING: ANSIColor.YELLOW,
    logging.ERROR: ANSIColor.RED,
    logging.CRITICAL: ANSIColor.BRIGHT_RED,
}


_METHOD_COLORS: dict[str, ANSIColor] = {
    "GET": ANSIColor.GREEN,
    "POST": ANSIColor.BLUE,
    "PUT": ANSIColor.YELLOW,
    "PATCH": ANSIColor.MAGENTA,
    "DELETE": ANSIColor.RED,
    "HEAD": ANSIColor.CYAN,
    "OPTIONS": ANSIColor.BRIGHT_CYAN,
}


def supports_color(stream: TextIO | None = None) -> bool:
    """
    Return True if ANSI colours should be emitted.
    """

    if stream is None:
        stream = sys.stderr

    return stream.isatty()


def colorize(
    text: str,
    color: ANSIColor,
    enabled: bool,
) -> str:
    """
    Wrap text in ANSI escape sequences.
    """

    if not enabled:
        return text

    return f"{color}{text}{ANSIColor.RESET}"


def level_color(level: int) -> ANSIColor:
    """Return the ANSI colour for a log level."""

    return _LEVEL_COLORS.get(level, ANSIColor.WHITE)


def method_color(method: str) -> ANSIColor:
    """Return the ANSI colour for an HTTP method."""

    return _METHOD_COLORS.get(method.upper(), ANSIColor.WHITE)


def status_color(status_code: int) -> ANSIColor:
    """Return the ANSI colour for an HTTP status code."""

    if 100 <= status_code < 200:
        return ANSIColor.BRIGHT_CYAN

    if 200 <= status_code < 300:
        return ANSIColor.GREEN

    if 300 <= status_code < 400:
        return ANSIColor.CYAN

    if 400 <= status_code < 500:
        return ANSIColor.YELLOW

    return ANSIColor.RED