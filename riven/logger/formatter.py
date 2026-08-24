"""
Logging formatters used by Riven.
"""

from __future__ import annotations

from copy import copy
import logging

from .colors import (
    colorize,
    level_color,
    method_color,
    status_color,
    supports_color,
    status_phrase
)


class DefaultFormatter(logging.Formatter):
    """
    Default formatter used for server logs.
    """

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        *,
        use_colors: bool | None = None,
    ) -> None:
        super().__init__(fmt=fmt, datefmt=datefmt)

        self.use_colors = (
            supports_color()
            if use_colors is None
            else use_colors
        )

    def format(self, record: logging.LogRecord) -> str:
        record = copy(record)

        level = f"{record.levelname:<9}"

        record.__dict__["levelprefix"] = colorize(
            level,
            level_color(record.levelno),
            self.use_colors,
        )

        return super().format(record)


class AccessFormatter(DefaultFormatter):
    """
    Formatter used for HTTP access logs.
    """

    def format(self, record: logging.LogRecord) -> str:
        record = copy(record)

        level = f"{record.levelname:<9}"

        record.__dict__["levelprefix"] = colorize(
            level,
            level_color(record.levelno),
            self.use_colors,
        )

        method = record.__dict__.get("method")
        status_code = record.__dict__.get("status_code")

        if method is not None:
            record.__dict__["method"] = colorize(
                str(method),
                method_color(str(method)),
                self.use_colors,
            )

        if status_code:
            record.__dict__["status_code"] = (
                f"{colorize(
                    str(status_code),
                    status_color(int(status_code)),
                    self.use_colors,
                )} {status_phrase(int(status_code))}"
            )
        else:
            record.__dict__["status_code"] = ""

        request_line = f"{record.__dict__["path"]} {record.__dict__["http_version"]}"
        record.__dict__["request_line"] = request_line

        return super(DefaultFormatter, self).format(record)