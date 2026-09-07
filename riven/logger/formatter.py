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
        recordcopy = copy(record)

        level = f"{recordcopy.levelname:<9}"

        recordcopy.__dict__["levelprefix"] = colorize(
            level,
            level_color(recordcopy.levelno),
            self.use_colors,
        )

        return super().format(recordcopy)


class AccessFormatter(DefaultFormatter):
    """
    Formatter used for HTTP access logs.
    """

    def format(self, record: logging.LogRecord) -> str:
        recordcopy = copy(record)

        (client_addr,
         method,
         full_path,
         http_version,
         status_code) = recordcopy.args

        if method is not None:
            method = colorize(
                str(method),
                method_color(str(method)),
                self.use_colors,
            )

        if status_code:
            status_code = (
                f"{colorize(
                    str(status_code),
                    status_color(int(status_code)),
                    self.use_colors,
                )} {status_phrase(int(status_code))}"
            )
        else:
            status_code = ""

        request_line = f"{method} {full_path} HTTP/{http_version}"
        recordcopy.__dict__.update(
            {
                "client_addr" : client_addr,
                "request_line" : request_line,
                "status_code" : status_code
            }
        )

        return super().format(recordcopy)