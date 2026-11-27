from __future__ import annotations

import asyncio
from collections import deque
from typing import TYPE_CHECKING

from aioquic.quic.connection import (
    stream_is_client_initiated,
    stream_is_unidirectional,
)
from rcp import (
    WebTransportBidirectionalStreamEventType,
    WebTransportClientBidirectionalStreamStarted,
    WebTransportClientUnidirectionalStreamStarted,
    WebTransportDataEventType,
    WebTransportDataReceived,
    WebTransportUnidirectionalStreamEventType,
)

if TYPE_CHECKING:
    from ._session_wt import WebTransportSessionProtocol

from enum import IntEnum

from ..exceptions import exceptions
from .error import (
    WebTransportApplicationErrorCode,
    encode_app_error,
)


class SessionState(IntEnum):
    HANDSHAKE_PENDING = 0
    SESSION_ACCEPTED = 1
    SESSION_REJECTED = 2
    SESSION_CLOSING = 3
    SESSION_CLOSED = 4


class ConnectStreamState(IntEnum):
    CONNECT_STREAM_OPEN = 0
    CONNECT_STREAM_500 = 1
    CONNECT_STREAM_ENDED = 2
    CONNECT_STREAM_RESET = 3


class RCPAppState(IntEnum):
    NOT_RUNNING = 0
    RUNNING = 1
    CLOSED_WITH_ERROR = 2
    CLOSED = 3


class SessionTeardownState(IntEnum):
    NOT_REQUESTED = 0
    STARTED = 1
    FINISHED = 2


def can_stop_sending(stream_id: int) -> bool:
    """Return True if STOP_SENDING can be sent on the stream."""
    return stream_is_client_initiated(stream_id) or not stream_is_unidirectional(stream_id)


def can_reset_stream(stream_id: int) -> bool:
    """Return True if RESET_STREAM can be sent on the stream."""
    return not (stream_is_client_initiated(stream_id) and stream_is_unidirectional(stream_id))

class WebTransportStream:
    _IS_UNIDIRECTIONAL = 1 << 0
    _IS_CLIENT_INITIATED = 1 << 1
    _READING_CLOSED = 1 << 2
    _WRITING_CLOSED = 1 << 3

    def __init__(self, stream_id: int, session_id: int, protocol: WebTransportSessionProtocol):
        self.stream_id = stream_id
        self.session_id = session_id
        self.protocol = protocol
        self._flags: int = 0
        self.stream_data_buffer: bytearray | None = None

    def _get_flag(self, mask: int) -> bool:
        return bool(self._flags & mask)

    def _set_flag(self, mask: int, value: bool) -> None:
        if value:
            self._flags |= mask
        else:
            self._flags &= ~mask

    def _feed_data(self, data: bytes) -> None:
        """Feed incoming data into the stream's read buffer."""
        if not data:
            return

        self.stream_data_buffer.extend(data)

    def _consume_buffer(self) -> bytes:
        """Consume all currently buffered stream data."""
        if self.stream_data_buffer is not None and not self.stream_data_buffer:
            return b""
        data = bytes(self.stream_data_buffer)
        self.stream_data_buffer.clear()
        return data

    def _buffer_is_empty(self) -> bool:
        return not self.stream_data_buffer

    @property
    def closed(self) -> bool:
        """Return True when both the reading and writing directions are fully closed
        (network-side and application-side).

        .. note::
            For unidirectional streams, the unavailable direction is considered
            closed by default, depending on the stream's initiator.
        """
        return (
            self.writing_closed
            and self.reading_closed
        )

    @property
    def is_unidirectional(self) -> bool:
        return self._get_flag(self._IS_UNIDIRECTIONAL)

    @is_unidirectional.setter
    def is_unidirectional(self, value: bool) -> None:
        self._set_flag(self._IS_UNIDIRECTIONAL, value)

    @property
    def is_client_initiated(self) -> bool:
        return self._get_flag(self._IS_CLIENT_INITIATED)

    @is_client_initiated.setter
    def is_client_initiated(self, value: bool) -> None:
        self._set_flag(self._IS_CLIENT_INITIATED, value)

    @property
    def reading_closed(self) -> bool:
        return self._get_flag(self._READING_CLOSED)

    @reading_closed.setter
    def reading_closed(self, value: bool) -> None:
        self._set_flag(self._READING_CLOSED, value)

    @property
    def writing_closed(self) -> bool:
        return self._get_flag(self._WRITING_CLOSED)

    @writing_closed.setter
    def writing_closed(self, value: bool) -> None:
        self._set_flag(self._WRITING_CLOSED, value)
