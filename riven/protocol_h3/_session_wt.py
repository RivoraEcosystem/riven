from __future__ import annotations

import asyncio
import contextlib
import logging
from asyncio import Queue, QueueShutDown
from collections.abc import Generator
from enum import IntEnum
from typing import TYPE_CHECKING

from aioquic.buffer import size_uint_var
from aioquic.h3.connection import ErrorCode
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
    WebTransportDatagramEventType,
    WebTransportDatagramReceived,
    WebtransportReceiveEvent,
    WebtransportSendEvent,
    WebTransportSessionDisconnect,
    WebTransportUnidirectionalStreamEventType,
    Writer,
)

from ..exceptions import exceptions
from ._stream_h3 import build_validate_headers, check_status, construct_pseudo_headers
from ._stream_wt import WebTransportStream, can_reset_stream, can_stop_sending, stream_is_unidirectional
from .capsules import (
    WT_CLOSE_SESSION,
)
from .error import WebTransportApplicationErrorCode, encode_app_error
from rcp.http.events import Headers

if TYPE_CHECKING:
    from ._protocol import RivenH3

WEBTRANSPORT_UNI_STREAM = 0x54
WEBTRANSPORT_BIDI_STREAM = 0x41


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


def webtransport_header_size(
    session_id: int,
    bidirectional: bool,
) -> int:
    stream_type = WEBTRANSPORT_BIDI_STREAM if bidirectional else WEBTRANSPORT_UNI_STREAM

    return size_uint_var(stream_type) + size_uint_var(session_id)


def get_webtransport_stream_header_size(stream_id: int, session_id: int):
    is_unidirectional = stream_is_unidirectional(stream_id)
    return webtransport_header_size(session_id=session_id, bidirectional=(not is_unidirectional))


def is_2xx_status(code: int) -> bool:
    return 200 <= code <= 299


access_logger = logging.getLogger("riven.access")
protocol_logger = logging.getLogger("riven.protocol")


class WebTransportSessionProtocol:
    """
    WebTransport Session Protocol to Manage WebTransport Session streams and Datagrams using H3Protocol.
    """

    __slots__ = (
        "_protocol",
        "_quic",
        "_http",
        "config",
        "scope",
        "server_state",
        "_queue",
        "_buffer",
        "_stream_ids",
        "session_state",
        "connect_stream_state",
        "rcp_app_state",
        "disconnect_event",
        "session_teared_down",
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

        self._stream_ids: set[int] = set()

        # session state
        self.session_state: SessionState = SessionState.HANDSHAKE_PENDING

        # connect stream state
        self.connect_stream_state: ConnectStreamState = ConnectStreamState.CONNECT_STREAM_OPEN

        # RCP Application state
        self.rcp_app_state: RCPAppState = RCPAppState.NOT_RUNNING

        # session disconnect info
        self.disconnect_event: WebTransportSessionDisconnect | None = None

        # status of session teardown after connect stream ending/reset
        self.session_teared_down: bool = False

        self.session_id: int | None = None

    def handle_webtransport_event(self, event) -> None:
        """
        Handle WebTransport specific events.
        """
        if self.connect_stream_state != ConnectStreamState.CONNECT_STREAM_OPEN:
            if not self.session_teared_down:
                # TODO: session teardown logic after CONNECT stream reset or ending
                ...

            return

        if isinstance(event, DataReceived):
            self._buffer += event.data
            for capsule in self._http.parse_webtransport_capsule(self):
                if isinstance(capsule, WT_CLOSE_SESSION):
                    self.close_session(capsule)
        elif isinstance(event, WebTransportStreamDataReceived):
            if self.rcp_app_state in (RCPAppState.CLOSED_WITH_ERROR, RCPAppState.CLOSED):
                self.terminate_wt_stream(stream_id=event.stream_id)
                return

            try:
                with self._get_or_create_wt_stream(event.stream_id) as stream:
                    if event.stream_ended:
                        stream.reading_closed = True

                    stream._feed_data(data=event.data)
                return

            except (exceptions.InvalidStream, ValueError) as exc:
                # error in internal stream mapping not a protocol Error close the stream's both ends
                self.terminate_wt_stream(stream_id=event.stream_id)

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
    def _get_or_create_wt_stream(self, stream_id: int) -> Generator[WebTransportStream, None, None]:

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
        self.rcp_app_state = RCPAppState.RUNNING

    def close_session(self, capsule: WT_CLOSE_SESSION): ...

    async def run_rcp(self, app: RCPApplication) -> None:
        try:
            result = await app(self.scope, self.receive, self.send)

        except Exception as exc:
            msg = "Exception in RCP application\n"
            protocol_logger.error(msg, exc_info=exc)
            # session handshake still pending send 500 response
            if self.session_state == SessionState.HANDSHAKE_PENDING:
                self.send_500_response()
            else:
                # session handshake is completed already cannot send 500 reset the connect stream
                self.reset_session_stream()

            self.rcp_app_state = RCPAppState.CLOSED_WITH_ERROR

        else:
            if self.session_state == SessionState.HANDSHAKE_PENDING:
                msg = "RCP Application returned without completing webtransport handshake."
                self.send_500_response()
                protocol_logger.error(msg)
                self.rcp_app_state = RCPAppState.CLOSED_WITH_ERROR

            elif result is not None:
                msg = f"RCP Application should return None, but returned {result}."
                self.reset_session_stream()
                protocol_logger.error(msg)
                self.rcp_app_state = RCPAppState.CLOSED_WITH_ERROR

            else:
                self.rcp_app_state = RCPAppState.CLOSED
                self._http.send_data(stream_id=self.session_id, data=b"", end_stream=True)
                self._protocol.transmit()

        finally:
            if not self.session_teared_down:
                # TODO: Session terdown logic after app ending or CONNECT stream end
                ...

    def reset_session_stream(self, error_code: ErrorCode = ErrorCode.H3_INTERNAL_ERROR):
        # stream already reset/closed or Connection is closed no need to reset the stream
        if (
            self._protocol._disconnected
            or self.connect_stream_state != ConnectStreamState.CONNECT_STREAM_OPEN
        ):
            return

        self._quic.reset_stream(stream_id=self.session_id, error_code=error_code)
        self._protocol.transmit()
        self.connect_stream_state = ConnectStreamState.CONNECT_STREAM_RESET

    def send_500_response(self):
        self._http.send_headers(
            stream_id=self.session_id, headers=construct_pseudo_headers(500), end_stream=True
        )
        self._protocol.transmit()
        self.connect_stream_state = ConnectStreamState.CONNECT_STREAM_500

    def terminate_wt_stream(
        self, stream_id: int, error_code: int = WebTransportApplicationErrorCode.WT_APP_INTERNAL_ERROR
    ):
        if can_stop_sending(stream_id):
            self._quic.stop_stream(
                stream_id=stream_id,
                error_code=encode_app_error(error_code),
            )

        if can_reset_stream(stream_id):
            self._quic.reset_stream_at(
                stream_id=stream_id,
                error_code=encode_app_error(error_code),
                reliable_size=get_webtransport_stream_header_size(
                    stream_id=stream_id, session_id=self.session_id
                ),
            )

        self._protocol.transmit()

    def send_quic_stream_data(
        self,
        stream_id: int,
        data: bytes,
        end_stream: bool = False,
    ) -> None:
        """Send data on a QUIC stream and transmit immediately."""
        self._protocol.send_quic_stream_data(
            stream_id=stream_id,
            data=data,
            end_stream=end_stream,
        )


    def send_http_headers(
        self,
        stream_id: int,
        headers: Headers,
        end_stream: bool = False,
    ) -> None:
        """Send HTTP headers on a stream and transmit immediately."""
        self._protocol.send_http_headers(
            stream_id=stream_id,
            headers=headers,
            end_stream=end_stream,
        )


    def send_http_data(
        self,
        stream_id: int,
        data: bytes,
        end_stream: bool = False,
    ) -> None:
        """Send HTTP data on a stream and transmit immediately."""
        self._protocol.send_http_data(
            stream_id=stream_id,
            data=data,
            end_stream=end_stream,
        )

    def stop_stream(
        self,
        stream_id:int,
        error_code:int
    ) -> None:
        self._protocol.stop_stream(
            stream_id=stream_id,
            error_code=error_code
        )

    def reset_stream(
        self,
        stream_id:int,
        error_code:int
    ) -> None:
        self._protocol.reset_stream(
            stream_id=stream_id,
            error_code=error_code
        )

    async def receive(self) -> None:
        try:
            if self.connect_stream_state != ConnectStreamState.CONNECT_STREAM_OPEN:
                return self.disconnect_event

            event = await self._queue.get()
            return event

        except QueueShutDown:
            if self.connect_stream_state != ConnectStreamState.CONNECT_STREAM_OPEN:
                return self.disconnect_event

    async def send(self, event: WebtransportSendEvent) -> None | Writer | tuple[Writer, Reader]:

        if event["type"] == WebTransportConnectionEventType.ACCEPT:
            # application cannot accept session if session is not in handshake pending state
            if self.session_state != SessionState.HANDSHAKE_PENDING:
                raise RuntimeError(
                    f"RCP Violation - Cannot ACCEPT WebTransport session. "
                    f"Session is not in HANDSHAKE_PENDING state "
                    f"(current state: {self.session_state!r})"
                )

            headers = construct_pseudo_headers(200) + build_validate_headers(event.get("headers", []))

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

        elif event["type"] == WebTransportConnectionEventType.CLOSE:
            if self.session_state not in (SessionState.SESSION_ACCEPTED, SessionState.SESSION_CLOSING):
                raise RuntimeError(
                    f"RCP Violation - Cannot CLOSE WebTransport session. "
                    f"Session is not in accepted/closing state."
                    f"(current state: {self.session_state!r})"
                )

            reason = event.get("reason", "")
            if not isinstance(reason, str):
                raise exceptions.InvalidEventField(field="reason", got=type(reason), expected=str)

            raw_reason = reason.encode("utf-8")

            if len(raw_reason) > 1024:
                raise RuntimeError(
                    f"RCP Violation - Close reason exceeds 1024 bytes.length : {len(raw_reason)}"
                )

            error_code = event.get("error_code", 0)

            if isinstance(error_code, bool) or not isinstance(error_code, int):
                raise exceptions.InvalidEventField(field="error_code", got=type(error_code), expected=int)

            try:
                wire_code = encode_app_error(error_code)  # noqa: F841
            except ValueError as exc:
                raise RuntimeError("RCP Violation - Close error code exceed 4 bytes") from exc

            # TODO: WT_CLOSE_SESSION sending logic

            self.session_state = SessionState.SESSION_CLOSED
            return

        elif event["type"] == WebTransportDatagramEventType.SEND:
            if self.session_state != SessionState.SESSION_ACCEPTED:
                raise RuntimeError(
                    f"RCP Violation - Cannot send DATAGRAM. "
                    f"Session must be ACCEPTED before sending "
                    f"(current state: {self.session_state!r})"
                )

            data = event["data"]

            if not isinstance(data, bytes):
                raise exceptions.InvalidEventField(field="data", got=type(data), expected=bytes)

            self._http.send_datagram(stream_id=self.session_id, data=data)
            return

        elif event["type"] == WebTransportUnidirectionalStreamEventType.SERVER:
            if self.session_state != SessionState.SESSION_ACCEPTED:
                raise RuntimeError(
                    f"RCP Violation - Cannot start UNIDIRECTIONAL stream. "
                    f"Session must be ACCEPTED before starting "
                    f"(current state: {self.session_state!r})"
                )

            stream_id = self._http.create_webtransport_stream(
                session_id=self.session_id,
                is_unidirectional=True,
            )

            with self._get_or_create_wt_stream(stream_id=stream_id) as stream:
                return stream.writer

        elif event["type"] == WebTransportBidirectionalStreamEventType.SERVER:
            if self.session_state != SessionState.SESSION_ACCEPTED:
                raise RuntimeError(
                    f"RCP Violation - Cannot start BIDIRECTIONAL stream. "
                    f"Session must be ACCEPTED before starting "
                    f"(current state: {self.session_state!r})"
                )

            stream_id = self._http.create_webtransport_stream(
                session_id=self.session_id,
                is_unidirectional=False,
            )

            with self._get_or_create_wt_stream(stream_id=stream_id) as stream:
                return stream.writer, stream.reader

        else:
            raise RuntimeError(f"RCP Violation - Unexpected {event['type']!r} event sent")
