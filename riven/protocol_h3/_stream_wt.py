from __future__ import annotations

import asyncio
from collections import deque
from aioquic.h3.connection import ErrorCode

from aioquic.quic.connection import (
    stream_is_client_initiated,
    stream_is_unidirectional,
)
from rcp import (
    Reader,
    WebTransportDataEventType,
    WebTransportDataReceived,
    WebTransportStreamReadEvent,
    WebTransportStreamWriteEvent,
    Writer,
    WebTransportClientBidirectionalStreamStarted,
    WebTransportClientUnidirectionalStreamStarted,
    WebTransportUnidirectionalStreamEventType,
    WebTransportBidirectionalStreamEventType
)
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ._session_wt import WebTransportSessionProtocol

from ..exceptions import exceptions
from .error import WebTransportApplicationErrorCode , WebTransportErrorCode , encode_app_error , decode_app_error 


class StreamReaderClosed(exceptions.RivenException):
    """Raised when stream reader are used after closing"""


class StreamWriterClosed(exceptions.RivenException):
    """Raised when stream writer are used after closing"""


class StreamReadingError(exceptions.RivenException):
    """Raised when error occurs during reading the stream"""


class StreamWritingError(exceptions.RivenException):
    """Raised when error occurs during writing the stream"""

def can_stop_sending(stream_id: int) -> bool:
    """Return True if STOP_SENDING can be sent on the stream."""
    return (
        stream_is_client_initiated(stream_id)
        or not stream_is_unidirectional(stream_id)
    )

def can_reset_stream(stream_id: int) -> bool:
    """Return True if RESET_STREAM can be sent on the stream."""
    return not (
        stream_is_client_initiated(stream_id)
        and stream_is_unidirectional(stream_id)
    )

class StreamReader(Reader):
    def __init__(self, stream: WebTransportStream):
        self.stream = stream
        self.waiters: deque[asyncio.Future[None]] = deque()
        super().__init__()

    async def read(self) -> WebTransportStreamReadEvent:
        while True:
            if self.is_closed:
                raise StreamReaderClosed("Cannot read from a closed stream")

            if self.stream.stream_data_buffer:
                break

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
            "data": self.stream._consume_buffer(),
        }
        if self.stream.reading_closed and self.stream._buffer_is_empty():
            # close reading side when network reading side is closed and buffer is fully consumed
            self._closed = True

        return event

    async def close(self) -> None:
        self._closed = True
        self.wake_all_waiters()
        self.stream.protocol._quic.stop_stream(
            stream_id=self.stream.stream_id,
            error_code=encode_app_error(WebTransportApplicationErrorCode.WT_APP_STOP_SENDING_BY_APPLICATION)
        )
        self.stream.protocol._protocol.transmit()

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

    async def write(self, event: WebTransportStreamWriteEvent) -> None:
        if self.is_closed:
            raise StreamWriterClosed("Cannot write on a closed stream")
        
        if event['type'] == WebTransportDataEventType.SEND:
            data = event.get('data',b"")

            if not isinstance(data,bytes):
                raise exceptions.InvalidEventField(field="WebTransport stream data",got=type(data),expected=bytes)

            self.stream.protocol._quic.send_stream_data(
                stream_id=self.stream.stream_id,
                data=data,
                end_stream=False
            )
            self.stream.protocol._protocol.transmit()
            
        else:
            raise RuntimeError(f"RCP Violation - Unexpected {event['type']!r} event sent")

    async def close(self) -> None:
        "Gracefully end stream by setting FIN bit"
        self._closed = True
        self.stream._consume_buffer() # consume remaining data in buffer
        self.stream.protocol._quic.send_stream_data(
            stream_id=self.stream.stream_id,
            data=b"",end_stream=True
        )
        self.stream.protocol._protocol.transmit()


class WebTransportStream:
    _IS_UNIDIRECTIONAL = 1 << 0
    _IS_CLIENT_INITIATED = 1 << 1
    _READING_CLOSED = 1 << 2
    _WRITING_CLOSED = 1 << 3

    def __init__(self, stream_id: int, session_id: int, protocol:WebTransportSessionProtocol):
        self.stream_id = stream_id
        self.session_id = session_id
        self.protocol = protocol
        self._flags: int = 0
        self.stream_data_buffer: bytearray | None = None
        self.reader: StreamReader | None = None
        self.writer: StreamWriter | None = None

        self.init_stream()

    def init_stream(self) -> None:

        self.is_unidirectional = stream_is_unidirectional(self.stream_id)
        self.is_client_initiated = stream_is_client_initiated(self.stream_id)
        if self.is_unidirectional:
            if self.is_client_initiated:
                # server can only read client initiated unidirectional streams
                self.reader = StreamReader(self)
                self.stream_data_buffer = bytearray()
                # writing is automatically closed for client initiated unidirectional streams
                self.writing_closed = True
                self.post_stream_created_event()
                return
            self.writer = StreamWriter(self)  # servers can only write server initiated unidirectional streams
            # reading is automatically closed for server initiated unidirectional streams
            self.reading_closed = True
            return

        # bidirectional streams have both reader and writer
        self.stream_data_buffer = bytearray()
        self.reader = StreamReader(self)
        self.writer = StreamWriter(self)
        self.post_stream_created_event()

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

        if self.reader.is_closed: # reader closed drop the buffered data events
            return
        
        self.stream_data_buffer.extend(data)
        self.reader.wake_waiter()
    
    def _consume_buffer(self) -> bytes:
        """Consume all currently buffered stream data."""
        if self.stream_data_buffer is not None and not self.stream_data_buffer:
            return b""
        data = bytes(self.stream_data_buffer)
        self.stream_data_buffer.clear()
        return data

    def _buffer_is_empty(self) -> bool:
        return not self.stream_data_buffer

    def post_stream_created_event(self) -> None:
        if not self.is_client_initiated:
            return    

        if self.is_unidirectional:
            event: WebTransportClientUnidirectionalStreamStarted = {
                "type": WebTransportUnidirectionalStreamEventType.CLIENT,
                "reader": self.reader,
            }
        else:
            event: WebTransportClientBidirectionalStreamStarted = {
                "type": WebTransportBidirectionalStreamEventType.CLIENT,
                "reader": self.reader,
                "writer": self.writer,
            }

        self.protocol._queue.put_nowait(event)

    @property
    def closed(self) -> bool:
        """Return True when both the reading and writing directions are fully closed
        (network-side and application-side).

        .. note::
            For unidirectional streams, the unavailable direction is considered
            closed by default, depending on the stream's initiator.
        """
        return (
            self.writing_closed and
            self.reading_closed and
            (self.reader is None or self.reader.is_closed) and
            (self.writer is None or self.writer.is_closed)
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
        self._set_flag(self._READING_CLOSED,value)

    @property
    def writing_closed(self) -> bool:
        return self._get_flag(self._WRITING_CLOSED)

    @writing_closed.setter
    def writing_closed(self,value: bool) -> None:
        self._set_flag(self._WRITING_CLOSED,value)