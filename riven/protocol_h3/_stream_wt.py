from __future__ import annotations

import asyncio
from collections import deque

from aioquic.quic.connection import (
    stream_is_client_initiated,
    stream_is_unidirectional,
)
from rcp import (
    Reader,
    WebTransportDataEventType,
    WebTransportDataReceived,
    WebTransportDataSend,
    WebTransportStreamReadEvent,
    WebTransportStreamWriteEvent,
    Writer,
)

from ..exceptions import exceptions


class StreamReaderClosed(exceptions.RivenException):
    """Raised when stream reader are used after closing"""


class StreamWriterClosed(exceptions.RivenException):
    """Raised when stream writer are used after closing"""


class StreamReadingError(exceptions.RivenException):
    """Raised when error occurs during reading the stream"""


class StreamWritingError(exceptions.RivenException):
    """Raised when error occurs during writing the stream"""


class StreamReader(Reader):
    def __init__(self, stream: WebTransportStream):
        self.stream = stream
        self.waiters: deque[asyncio.Future[None]] = deque()
        super().__init__()

    async def read(self) -> WebTransportStreamReadEvent:
        while self.stream.stream_data_buffer == b"":
            if self.is_closed:
                raise StreamReaderClosed

            read_waiter = asyncio.get_running_loop().create_future()
            self.waiters.append(read_waiter)
            try:
                await read_waiter
            except asyncio.CancelledError:
                try:
                    self.waiters.remove(read_waiter)
                except ValueError:
                    pass

                if self.stream.stream_data_buffer:  # data still exists in buffer wakeup next consumer
                    self.wake_waiter()

                raise
        event: WebTransportDataReceived = {
            "type": WebTransportDataEventType.RECEIVED,
            "data": self.stream.read_data(),
        }
        return event

    async def close(self) -> None:
        self._closed = True
        self.wake_all_waiters()

    def wake_waiter(self) -> None:
        while self.waiters:
            waiter = self.waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                break

    def wake_all_waiters(self) -> None:
        while self.waiters:
            waiter = self.waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)


class StreamWriter(Writer):
    def __init__(self, stream: WebTransportStream):
        self.stream = stream
        super().__init__()

    async def write(self, event: WebTransportStreamWriteEvent) -> None: ...

    async def close(self) -> None: ...


class WebTransportStream:
    _DISCONNECTED = 1 << 0
    _IS_UNIDIRECTIONAL = 1 << 1
    _IS_CLIENT_INITIATED = 1 << 2

    def __init__(self, stream_id: int):
        self.stream_id = stream_id

        self._flags: int = 0
        self.stream_data_buffer: bytearray | None = None
        self.reader: StreamReader | None = None
        self.writer: StreamWriter | None = None

        self.init_stream()

    def init_stream(self) -> None:

        self.is_unidirectional = True if stream_is_unidirectional(self.stream_id) else False
        self.is_client_initiated = True if stream_is_client_initiated(self.stream_id) else False

        if self.is_unidirectional:
            if self.is_client_initiated:
                # server can only read client initiated unidirectional streams
                self.reader = StreamReader(self)
                self.stream_data_buffer = bytearray()
                return
            self.writer = StreamWriter(self)  # servers can only write server initiated unidirectional streams
            return

        # bidirectional streams have both reader and writer
        self.stream_data_buffer = bytearray()
        self.reader = StreamReader(self)
        self.writer = StreamWriter(self)

    def _get_flag(self, mask: int) -> bool:
        return bool(self._flags & mask)

    def _set_flag(self, mask: int, value: bool) -> None:
        if value:
            self._flags |= mask
        else:
            self._flags &= ~mask

    def write_data(self, data: bytes):
        if not data:
            return

        if self.stream_data_buffer is None:
            raise StreamReadingError("cannot read on server initiated unidirectional stream")

        self.stream_data_buffer.extend(data)
        self.reader.wake_waiter()  # wake waiters if any

    def read_data(self) -> bytes:  # return all data in buffer and clean the buffer
        data = bytes(self.stream_data_buffer)
        self.stream_data_buffer.clear()
        return data

    @property
    def disconnected(self) -> bool:
        return self._get_flag(self._DISCONNECTED)

    @disconnected.setter
    def disconnected(self, value: bool) -> None:
        self._set_flag(self._DISCONNECTED, value)

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
