# ruff: noqa: F401
from __future__ import annotations

import asyncio
import contextlib
import logging
from asyncio import Queue, QueueShutDown
from collections.abc import Generator
from enum import IntEnum
from typing import TYPE_CHECKING

from aioquic.h3.events import (
    DatagramReceived,
    DataReceived,
    WebTransportStreamDataReceived,
)
from rcp import (
    WEBTRANSPORT_EXTENSION,
    RCPApplication,
    Reader,
    WebTransportBidirectionalStreamEventType,
    WebTransportConnectionEventType,
    WebTransportCreateServerBidirectionalStream,
    WebTransportCreateServerUnidirectionalStream,
    WebTransportDatagramEventType,
    WebTransportDatagramReceived,
    WebTransportDatagramSend,
    WebtransportReceiveEvent,
    WebtransportSendEvent,
    WebTransportSessionAccept,
    WebTransportSessionClose,
    WebTransportSessionDisconnect,
    WebTransportSessionReject,
    WebTransportUnidirectionalStreamEventType,
    Writer,
)

from ..exceptions import exceptions
from ._stream_h3 import build_validate_headers, check_status, construct_pseudo_headers
from ._stream_wt import WebTransportStream, can_reset_stream, can_stop_sending
from .capsules import (
    WT_CLOSE_SESSION,
)
from .error import WebTransportApplicationErrorCode, WebTransportErrorCode, decode_app_error, encode_app_error

if TYPE_CHECKING:
    from ._protocol import RivenH3


class SessionState(IntEnum):
    HANDSHAKE_PENDING = 0
    SESSION_ACCEPTED = 1
    SESSION_REJECTED = 2
    SESSION_CLOSING = 3
    SESSION_CLOSED = 4


def is_2xx_status(code: int) -> bool:
    return 200 <= code <= 299


access_logger = logging.getLogger("riven.access")
protocol_logger = logging.getLogger("riven.protocol")


class WebTransportSessionProtocol:
    """
    WebTransport Session Protocol to Manage WebTransport Session streams and Datagrams using H3Protocol.
    """

    _CLOSED = 1 << 0
    _DISCONNECTED = 1 << 1
    _APP_ENDED = 1 << 3

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
        "session_state",
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

        # session state
        self.session_state: SessionState = SessionState.HANDSHAKE_PENDING

        # session disconnect info
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
    def disconnected(self) -> bool:
        return self._get_flag(self._DISCONNECTED)

    @disconnected.setter
    def disconnected(self, value: bool) -> None:
        self._set_flag(self._DISCONNECTED, value)

    @property
    def app_ended(self) -> bool:
        return self._get_flag(self._APP_ENDED)

    @app_ended.setter
    def app_ended(self, value: bool) -> None:
        self._set_flag(self._APP_ENDED, value)

    def handle_webtransport_event(self, event) -> None:
        """
        Handle WebTransport specific events.
        """
        if self.app_ended:
            # TODO: session teardown logic after CONNECT stream reset or ending
            ...

        if isinstance(event, DataReceived):
            self._buffer += event.data
            for capsule in self._http.parse_webtransport_capsule(self):
                if isinstance(capsule, WT_CLOSE_SESSION):
                    self.close_session(capsule)
        elif isinstance(event, WebTransportStreamDataReceived):
            try:
                with self._get_or_create_wt_stream(event.stream_id) as stream:
                    if event.stream_ended:
                        stream.reading_closed = True

                    stream._feed_data(data=event.data)
                return

            except (exceptions.InvalidStream, ValueError) as exc:
                # error in internal stream mapping not a protocol Error close the stream's both ends
                if can_stop_sending(event.stream_id):
                    self._quic.stop_stream(
                        stream_id=event.stream_id,
                        error_code=encode_app_error(WebTransportApplicationErrorCode.WT_APP_INTERNAL_ERROR),
                    )
                if can_reset_stream(event.stream_id):
                    # TODO: add stream_reset_at logic
                    ...

                self._protocol.transmit()
                protocol_logger.error(
                    "Failed to handle WebTransportStreamDataReceived event for Webtransport stream "
                    "stream %d - Invalid Stream Type or Stream Already Exists",
                    event.stream_id,
                    exc_info=exc,
                )

        elif isinstance(event, DatagramReceived):
            event: WebTransportDatagramReceived = {
                "type": WebTransportDatagramEventType.RECEIVED,
                "data": event.data,
            }
            self._queue.put_nowait(event)
            return

    def _wt_stream_created(self, stream_id: int):
        if stream_id in self._stream_ids:
            raise ValueError(f"{stream_id} already exists")
        self._stream_ids.add(stream_id)

    def _wt_stream_ended(self, stream_id: int):
        self._stream_ids.discard(stream_id)

    @contextlib.contextmanager
    def _get_or_create_wt_stream(self, stream_id: int) -> Generator[WebTransportStream]:

        stream = self._protocol._active_streams_sessions.get(stream_id)

        if stream is None:
            stream = WebTransportStream(stream_id=stream_id, session_id=self.session_id, protocol=self)
            self._protocol._active_streams_sessions[stream_id] = stream
            self._wt_stream_created(stream_id)

        elif not isinstance(stream, WebTransportStream):
            raise exceptions.InvalidStream(stream, WebTransportStream)

        try:
            yield stream
        finally:
            # remove stream if closed
            if stream.closed:
                self._protocol.remove_stream(stream_id=stream.stream_id, remover=self)
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

        if event["type"] == WebTransportConnectionEventType.ACCEPT:
            # application cannot accept session if session is not in handshake pending state
            if self.session_state != SessionState.HANDSHAKE_PENDING:
                raise RuntimeError(
                    f"RCP Violation - Cannot ACCEPT WebTransport session. "
                    f"Session is not in HANDSHAKE_PENDING state "
                    f"(current state: {self.session_state!r})"
                )

            headers = construct_pseudo_headers(200) + build_validate_headers(event.get('headers',[]))

            self._http.send_headers(stream_id=self.session_id, headers=headers)
            self.session_state = SessionState.SESSION_ACCEPTED
            return

        elif event["type"] == WebTransportConnectionEventType.REJECT:
            # application cannot reject session if session is not in handshake pending state
            if self.session_state != SessionState.HANDSHAKE_PENDING:
                raise RuntimeError(
                    f"RCP Violation - Cannot REJECT WebTransport session. "
                    f"Session is not in HANDSHAKE_PENDING state "
                    f"(current state: {self.session_state!r})"
                )

            status = event["status"]
            if not check_status(status):
                raise RuntimeError(
                    f"RCP Violation - Invalid HTTP status code "
                    f"{status!r} provided by application for "
                    f"{event['type']!r} event"
                )

            if is_2xx_status(status):
                raise RuntimeError(
                    f"RCP Violation - Cannot REJECT WebTransport session "
                    f"with a 2xx HTTP status code ({status})"
                )

            headers = construct_pseudo_headers(status)

            self._http.send_headers(stream_id=self.session_id, headers=headers)
            self.session_state = SessionState.SESSION_REJECTED
            return

        elif event['type'] == WebTransportConnectionEventType.CLOSE:
            if self.session_state not in (
                SessionState.SESSION_ACCEPTED,
                SessionState.SESSION_CLOSING
            ):
                raise RuntimeError(
                    f"RCP Violation - Cannot CLOSE WebTransport session. "
                    f"Session is not in accepted/closing state."
                    f"(current state: {self.session_state!r})"
                )

            reason = event.get('reason',"")
            if not isinstance(reason, str):
                raise exceptions.InvalidEventField(
                    field="reason",
                    got=type(reason),
                    expected=str
                )
            
            raw_reason = reason.encode("utf-8")

            if len(raw_reason) > 1024:
                raise RuntimeError(
                    f"RCP Violation - Close reason exceeds 1024 bytes."
                    f"length : {len(raw_reason)}"
                )

            error_code = event.get('error_code',0)

            if isinstance(error_code, bool) or not isinstance(error_code,int):
                raise exceptions.InvalidEventField(
                    field='error_code',
                    got=type(error_code),
                    expected=int
                )

            try:
                wire_code = encode_app_error(error_code)
            except ValueError as exc:
                raise RuntimeError(
                    f"RCP Violation - Close error code exceed 4 bytes"
                ) from exc

            # TODO: WT_CLOSE_SESSION sending logic

            self.session_state = SessionState.SESSION_CLOSED
            return

        elif event['type'] == WebTransportDatagramEventType.SEND:
            
            if self.session_state != SessionState.SESSION_ACCEPTED:
                raise RuntimeError(
                    f"RCP Violation - Cannot send DATAGRAM. "
                    f"Session must be ACCEPTED before sending "
                    f"(current state: {self.session_state!r})"
                )

            data = event["data"]

            if not isinstance(data,bytes):
                raise exceptions.InvalidEventField(
                    field='data',
                    got=type(data),
                    expected=bytes
                )

            self._http.send_datagram(stream_id=self.session_id,data=data)
            return