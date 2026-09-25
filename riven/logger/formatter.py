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
    status_phrase,
    ANSIColor
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

        level = recordcopy.levelname
        separator = " " * (9 - len(level))
        recordcopy.__dict__["levelprefix"] = colorize(
            level,
            level_color(recordcopy.levelno),
            self.use_colors,
        ) +":" + separator
        if self.use_colors:
            if "color_message" in recordcopy.__dict__:
                recordcopy.msg = recordcopy.__dict__["color_message"]
                recordcopy.__dict__["message"] = recordcopy.getMessage() # re format message with colour arguments

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

        if status_code:
            status_code = (
                f"{colorize(
                    str(status_code),
                    status_color(int(status_code)),
                    self.use_colors,
                )} {colorize(status_phrase(int(status_code)),status_color(status_code),self.use_colors)}"
            )
        else:
            status_code = ""

        request_line = colorize(f"{method} {full_path} HTTP/{http_version}",ANSIColor.BRIGHT_BOLD_WHITE,self.use_colors)
        recordcopy.__dict__.update(
            {
                "client_addr" : client_addr,
                "request_line" : request_line,
                "status_code" : status_code
            }
        )

        return super().format(recordcopy)