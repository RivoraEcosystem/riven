from __future__ import annotations

import asyncio
import logging
from asyncio import Queue, QueueShutDown
from typing import TYPE_CHECKING

from aioquic.quic.connection import (
    stream_is_client_initiated,
    stream_is_unidirectional,
)
from aioquic.h3.events import (
    DatagramReceived,
    DataReceived,
    WebTransportStreamDataReceived,
)

import contextlib
from collections.abc import Generator

from rcp import (
    WEBTRANSPORT_EXTENSION,
    RCPApplication,
    Reader,
    WebtransportReceiveEvent,
    WebtransportSendEvent,
    WebTransportSessionDisconnect,
    Writer,
    WebTransportBidirectionalStreamEventType,
    WebTransportUnidirectionalStreamEventType
)

from ._stream_wt import WebTransportStream , can_reset_stream , can_stop_sending

from .capsules import (
    WT_CLOSE_SESSION,
)

from ..exceptions import exceptions
from .error import WebTransportApplicationErrorCode , WebTransportErrorCode , encode_app_error , decode_app_error

if TYPE_CHECKING:
    from ._protocol import RivenH3

access_logger = logging.getLogger("riven.access")
protocol_logger = logging.getLogger("riven.protocol")


class WebTransportSessionProtocol:
    """
    WebTransport Session Protocol to Manage WebTransport Session streams and Datagrams using H3Protocol.
    """

    _CLOSED = 1 << 0
    _STREAM_RESET = 1 << 1
    _STREAM_ENDED = 1 << 2

    __slots__ = (
        "_protocol",
        "_quic",
        "_http",
        "config",
        "scope",
        "server_state",
        "_queue",
        "_buffer",
        "_flags",
        "_stream_ids",
        "disconnected",
        "disconnect_event",
        "session_id",
    )

    def __init__(self, protocol: RivenH3, scope: dict):

        self._protocol = protocol
        self._quic = protocol._quic
        self._http = protocol._http
        self.config = protocol.config
        self.scope = scope
        self.server_state = protocol.server_state
        self._queue: Queue[WebtransportReceiveEvent] = Queue()

        self._buffer: bytes = b""

        self._flags: int = 0
        self._stream_ids: set[int] = set()

        # session disconnect info
        self.disconnected: bool = False
        self.disconnect_event: WebTransportSessionDisconnect | None = None
        self.session_id: int | None = None

    def _get_flag(self, mask: int) -> bool:
        return bool(self._flags & mask)

    def _set_flag(self, mask: int, value: bool) -> None:
        if value:
            self._flags |= mask
        else:
            self._flags &= ~mask

    @property
    def _closed(self) -> bool:
        return self._get_flag(self._CLOSED)

    @_closed.setter
    def _closed(self, value: bool) -> None:
        self._set_flag(self._CLOSED, value)

    @property
    def _stream_reset(self) -> bool:
        return self._get_flag(self._STREAM_RESET)

    @_stream_reset.setter
    def _stream_reset(self, value: bool) -> None:
        self._set_flag(self._STREAM_RESET, value)

    @property
    def _stream_ended(self) -> bool:
        return self._get_flag(self._STREAM_ENDED)

    @_stream_ended.setter
    def _stream_ended(self, value: bool) -> None:
        self._set_flag(self._STREAM_ENDED, value)

    def handle_webtransport_event(self, event) -> None:
        """
        Handle WebTransport specific events.
        """
        if isinstance(event, DataReceived):
            self._buffer += event.data
            for capsule in self._http.parse_webtransport_capsule(self):
                if isinstance(capsule, WT_CLOSE_SESSION):
                    self.close_session(capsule)
        elif isinstance(event,WebTransportStreamDataReceived):
            try:
                with self._get_or_create_wt_stream(event.stream_id) as stream:
                     
                    if event.stream_ended:
                        stream.reading_closed = True

                    stream._feed_data(data=event.data)

                return

            except (exceptions.InvalidStream , ValueError) as exc:
                # error in internal stream mapping not a protocol Error close the stream's both ends
                if can_stop_sending(event.stream_id):
                    self._quic.stop_stream(
                        stream_id=event.stream_id,
                        error_code=encode_app_error(
                            WebTransportApplicationErrorCode.WT_APP_INTERNAL_ERROR
                        )
                    )
                if can_reset_stream(event.stream_id):
                    # TODO: add stream_reset_at logic
                    ...
                    
                self._protocol.transmit()
                protocol_logger.error(
                    "Failed to handle WebTransportStreamDataReceived event for Webtransport stream "
                    "stream %d - Invalid Stream Type or Stream Already Exists",
                    event.stream_id,
                    exc_info=exc
                )

    def _wt_stream_created(self, stream_id: int):
        if stream_id in self._stream_ids:
            raise ValueError(f"{stream_id} already exists")
        self._stream_ids.add(stream_id)

    def _wt_stream_ended(self, stream_id: int):
        self._stream_ids.discard(stream_id)

    @contextlib.contextmanager
    def _get_or_create_wt_stream(self,stream_id:int) -> Generator[WebTransportStream]:
        
        stream = self._protocol._active_streams_sessions.get(stream_id)

        if stream is None:
            stream = WebTransportStream(
                stream_id=stream_id,
                session_id=self.session_id,
                protocol=self
            )
            self._protocol._active_streams_sessions[stream_id] = stream
            self._wt_stream_created(stream_id)
            

        elif not isinstance(stream, WebTransportStream):
            raise exceptions.InvalidStream(stream, WebTransportStream)

        try:
            yield stream
        finally:
            # remove stream if closed
            if stream.closed:
                self._protocol._active_streams_sessions.pop(stream.stream_id,None)
                self._wt_stream_ended(stream_id=stream.stream_id)

    def handle_session_start(self) -> None:
        """
        Handle the WebTransport Session establishment invoke RCP Application.
        """

        if self.config.application_interface == "asgi":
            self.scope["extensions"] = WEBTRANSPORT_EXTENSION

        app = self.config.loaded_application
        loop = asyncio.get_running_loop()

        app_task = loop.create_task(self.run_rcp(app=app))

        self.server_state.application_tasks.add(app_task)
        app_task.add_done_callback(self.server_state.application_tasks.discard)

    def close_session(self, capsule: WT_CLOSE_SESSION): ...

    async def run_rcp(self, app: RCPApplication) -> None:
        try:
            result = await app(self.scope, self.receive, self.send)

        except Exception as exc:
            msg = "Exception in RCP application\n"
            protocol_logger.error(msg, exc_info=exc)

        else:
            if result is not None:
                msg = f"RCP Application should return None, but returned {result}."
                protocol_logger.error(msg)

    async def receive(self) -> None:
        try:
            if self.disconnected:
                return self.disconnect_event

            event = await self._queue.get()
            return event

        except QueueShutDown:
            if self.disconnected:
                return self.disconnect_event

    async def send(self, event: WebtransportSendEvent) -> None | tuple[Writer, Reader]:
        try:
            ...
        except Exception:
            ...
