from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.buffer import Buffer
from aioquic.h3.connection import H3_ALPN, ErrorCode, FrameType, H3Connection, Setting, SettingsError
from aioquic.h3.events import (
    DataReceived,
    HeadersReceived,
)
from aioquic.quic.events import (
    ConnectionTerminated,
    ProtocolNegotiated,
    StopSendingReceived,
    StreamReset,
)
from rcp import (
    H3_FORBIDDEN_HEADERS,
    HTTPScheme,
    HTTPScope,
    HTTPVersions,
    RequestMethod,
    ScopeType,
    WebTransportScope,
)

from .._config import APPLICATION_INTERFACE_SPEC, RivenConfig
from ..exceptions import exceptions
from ..protocol_webtransport_h3._protocol_wt import WebTransportSettings
from ..protocol_webtransport_h3._session_wt import WebTransportSessionProtocol
from ._stream_utils import ConnectionInfo, HTTP3Stream

if TYPE_CHECKING:
    from ..server import RivenState

access_logger = logging.getLogger("riven.access")
protocol_logger = logging.getLogger("riven.protocol")


def send_goaway_and_disconnect(
    protocol: RivenH3, error_code: int = ErrorCode.H3_NO_ERROR, reason_phrase="Server Shutting down"
):
    """
    Encodes the GOAWAY frame using aioquic's Buffer and immediately queues
    a CONNECTION_CLOSE so they are packed into the same UDP envelope.
    """
    quic_conn = protocol._quic

    # USE H3_CONNECTION's internal local control stream id variable
    control_stream_id = protocol._http._local_control_stream_id

    # Encode HTTP/3 GOAWAY frame using aioquic's buffer class
    buf = Buffer(capacity=32)
    buf.push_uint_var(FrameType.GOAWAY)  # Frame Type (0x07)

    payload_buffer = Buffer(capacity=16)
    payload_buffer.push_uint_var(protocol.highest_seen_stream_id)
    payload_len = payload_buffer.tell()

    buf.push_uint_var(payload_len)
    buf.push_uint_var(protocol.highest_seen_stream_id)

    # Retrieve bytes cleanly by reading the data property
    # from index 0 up to where the pointer stopped (tell)
    goaway_frame_bytes = buf.data[: buf.tell()]

    # Queue GOAWAY frame
    quic_conn.send_stream_data(control_stream_id, data=goaway_frame_bytes)

    # Immediately queue connection level CONNECTION_CLOSE frame
    quic_conn.close(error_code=error_code, reason_phrase=reason_phrase)

    # Flush both frames together instantly into same network envelope
    protocol.transmit()


class RivenH3(QuicConnectionProtocol):
    """
    Riven HTTP/3 Quic protocol wrapper over aioquic QuicConnectionProtocol
    to handle HTTP/3 events and manage streams, connections, and application state.
    It also implements custom logic for handling headers, stream creation, and connection termination.
    """

    def __init__(
        self, config: RivenConfig, app_state: dict[str, Any], server_state: RivenState, *args, **kwargs
    ):
        if not config.loaded:
            config.load()
        super().__init__(*args, **kwargs)
        self.connection_id = self._quic.host_cid
        self.config = config
        self.app_state = app_state
        self.server_state = server_state
        self._active_streams_sessions: dict[int, HTTP3Stream] = dict()
        self._http = None
        self._disconnected = False
        self.server_state.add_connection(self)  # add connection to server state

        # highest value of seen streamID
        self.highest_seen_stream_id: int = 0

    def quic_event_received(self, event):
        if isinstance(event, ProtocolNegotiated):
            if event.alpn_protocol == H3_ALPN[0]:  # confirm HTTP3
                self._http = H3Protocol(
                    quic=self._quic, enable_webtransport=self.config.enable_webtransport
                )  # Upgrade to HTTP3

        elif isinstance(event, ConnectionTerminated):
            self._schedule_disconnect(event)

        elif isinstance(event, StreamReset) or isinstance(event, StopSendingReceived):
            self._handle_stream_interrupt(event)

        if self._http:
            for http_event in self._http.handle_event(event):
                if isinstance(http_event, HeadersReceived):
                    # event streamID value higher than stored value
                    if http_event.stream_id > self.highest_seen_stream_id:
                        self.highest_seen_stream_id = http_event.stream_id

                    self._create_stream(event=http_event)
                elif isinstance(http_event, DataReceived):
                    stream = self._active_streams_sessions.get(http_event.stream_id)

                    if not isinstance(stream, HTTP3Stream):
                        self._quic.reset_stream(
                            stream_id=http_event.stream_id, error_code=ErrorCode.H3_INTERNAL_ERROR
                        )
                        self.transmit()
                        protocol_logger.error(
                            "Failed to handle HTTP/3 DataReceived event for stream %d - Invalid Stream",
                            http_event.stream_id,
                        )
                        return

                    if stream._closed:
                        self._quic.reset_stream(
                            stream_id=http_event.stream_id, error_code=ErrorCode.H3_INTERNAL_ERROR
                        )
                        self.transmit()
                        stream._stream_reset = True
                        protocol_logger.error(
                            "Failed to handle HTTP/3 DataReceived event for stream %d - "
                            "Stream Closed By Application",
                            http_event.stream_id,
                        )
                        return

                    if http_event.stream_ended:
                        stream.more_body = False
                        stream._request_complete = True

                    stream._body += http_event.data  # add event data to stream's body

    def _create_stream(self, event: HeadersReceived) -> None:
        """
        Parse pseudo headers of H3 Events and create Stream and Call the application for valid requests,
        but reject requests that exceed the configured maximum header size
        """
        try:
            if not isinstance(event, HeadersReceived):
                raise exceptions.InvalidEvent(event)

            header_size = self.header_list_size(event.headers)

            if (
                self.config.max_header_size > 0  # rejecting if headers exceed limit
                and header_size > self.config.max_header_size
            ):
                if not self._disconnected:
                    self._http.send_headers(
                        stream_id=event.stream_id,
                        headers=[
                            (b":status", b"431"),
                            (b"content-length", b"0"),
                        ],
                        end_stream=True,
                    )

                    self.transmit()
                return

            method = None
            scheme = None
            raw_target = None
            headers = []
            authority = None
            normal_header = False
            host = None
            protocol = None

            # Iterate instead of using dict() to preserve duplicate HTTP headers
            # while validating pseudo-headers individually.
            for name, value in event.headers:
                if name != name.lower():
                    raise exceptions.H3MalformedMessage(f"Header name must be lowercase: {name!r}")

                if name.startswith(b":"):
                    if normal_header:
                        # Check RFC 9114 4.1.2 https://datatracker.ietf.org/doc/html/rfc9114#section-4.1.2
                        raise exceptions.H3MalformedMessage(
                            "Request Malformed - Pseudo headers after normal headers"
                        )

                    if name == b":method":
                        if method is not None:
                            raise exceptions.H3MalformedMessage("Duplicate pseudo header :method")
                        try:
                            method = RequestMethod(value)  # accept only Uppercase Methods
                            # reject CONNECT method if WebTransport is not enabled
                            if method == RequestMethod.CONNECT and not self.config.enable_webtransport:
                                raise exceptions.UnsupportedMethod("CONNECT")
                        except ValueError:
                            raise exceptions.MethodNotAllowed(value.decode("ascii", "replace"))

                    elif name == b":scheme":
                        if scheme is not None:
                            raise exceptions.H3MalformedMessage("Duplicate pseudo header :scheme")
                        try:
                            scheme = HTTPScheme(value)
                        except ValueError:
                            raise exceptions.InvalidScheme(value.decode("ascii", "replace"))

                    elif name == b":authority":
                        if authority is not None:
                            raise exceptions.H3MalformedMessage("Duplicate pseudo header :authority")
                        try:
                            authority = value.decode("ascii")
                        except UnicodeDecodeError:
                            raise exceptions.InvalidAuthority(value)

                    elif name == b":path":
                        if raw_target is not None:
                            raise exceptions.H3MalformedMessage("Duplicate pseudo header :path")
                        raw_target = value

                    elif name == b":protocol":
                        if protocol is not None:
                            raise exceptions.H3MalformedMessage("Duplicate pseudo header :protocol")
                        protocol = value

                    else:
                        raise exceptions.InvalidPseudoHeader(name.decode("ascii", "replace"))

                else:
                    if name == b"te" and not value.lower() == b"trailers":
                        raise exceptions.H3MalformedMessage("Invalid value for header: 'te'")

                    if name in H3_FORBIDDEN_HEADERS:
                        raise exceptions.H3MalformedMessage(f"Forbidden headers in request: {name!r}")

                    if name == b"host":  # store host header
                        host = value

                    headers.append((name, value))
                    normal_header = True  # normal headers started cant accept pseudo headers now

            if method is None:
                raise exceptions.MethodNotAllowed()

            if scheme in (HTTPScheme.HTTP, HTTPScheme.HTTPS):
                if authority is None or authority == "":
                    raise exceptions.InvalidAuthority()

            if raw_target is None:
                raise exceptions.InvalidPath()
            # reject authority and host header if both exists but their value dont match check :-
            # https://datatracker.ietf.org/doc/html/rfc9114#section-4.3.1-4
            if authority is not None and host is not None:
                if authority.encode("ascii") != host:
                    raise exceptions.H3MalformedMessage(":authority and Host do not match")

            # Validate :protocol pseudo header for WebTransport
            if protocol is not None and protocol != b"webtransport":
                raise exceptions.H3MalformedMessage(
                    "Invalid value for pseudo header :protocol - must be 'webtransport'"
                )

            if self.config.application_interface == "asgi" and not host:
                # add host header if application_interface is set to ASGI
                headers.append((b"host", authority.encode("ascii")))

            # Target validation
            is_asterisk_form = raw_target == b"*"

            if is_asterisk_form:
                if method != RequestMethod.OPTIONS:
                    raise exceptions.InvalidPath(raw_target)
            elif not (
                raw_target.startswith(b"/")
                or raw_target.startswith(b"http://")
                or raw_target.startswith(b"https://")
            ):
                raise exceptions.InvalidPath(raw_target)

            # Split path and query string
            raw_path, _, query_string = raw_target.partition(b"?")

            try:
                # convert to empty string if its a astrik
                path = "" if is_asterisk_form else raw_path.decode("utf-8")
            except UnicodeDecodeError:
                raise exceptions.InvalidPath(raw_path)

            client = self._quic._network_paths[0].addr  # client info from network paths
            server = self._transport.get_extra_info("sockname")  # server info from _transport

            if method == RequestMethod.CONNECT:
                if protocol != b"webtransport":
                    raise exceptions.H3MalformedMessage("CONNECT requires :protocol = webtransport")
                if event.stream_ended:
                    raise exceptions.WebTransportConnectStreamEnded(
                        "WebTransport CONNECT Stream ended before establishing session"
                    )

                scope: WebTransportScope = {
                    "type": ScopeType.WEBTRANSPORT,
                    "http_version": HTTPVersions.HTTP3,
                    "method": method,
                    "scheme": scheme,
                    "authority": authority,
                    "path": path,
                    "raw_path": raw_path,
                    "query_string": query_string,
                    "root_path": self.config.root_path,
                    "headers": headers,
                    "client": client,
                    "server": server,
                    "state": self.app_state.copy(),
                }
                if self.config.application_interface == "asgi":
                    scope["asgi"] = APPLICATION_INTERFACE_SPEC["asgi"]
                else:
                    scope["rcp"] = APPLICATION_INTERFACE_SPEC["rcp"]

                session = WebTransportSessionProtocol(protocol=self, scope=scope)

                # complete webtransport session handshake before adding it to active streams mapping
                session.handle_session_start()
                # add session to active sessions
                self._active_streams_sessions[event.stream_id] = session
                return

            else:
                http_scope: HTTPScope = {
                    "type": ScopeType.HTTP,
                    "http_version": HTTPVersions.HTTP3,
                    "method": method,
                    "scheme": scheme,
                    "authority": authority,
                    "path": path,
                    "raw_path": raw_path,
                    "query_string": query_string,
                    "root_path": self.config.root_path,
                    "headers": headers,
                    "client": client,
                    "server": server,
                    "state": self.app_state.copy(),
                }

                if self.config.application_interface == "asgi":
                    http_scope["asgi"] = APPLICATION_INTERFACE_SPEC["asgi"]
                else:
                    http_scope["rcp"] = APPLICATION_INTERFACE_SPEC["rcp"]

                connection = ConnectionInfo(
                    stream_id=event.stream_id,
                    server=server,
                    client=client,
                )

                stream = HTTP3Stream(  # build http stream
                    connection=connection,
                    stream_id=event.stream_id,
                    scope=http_scope,
                    protocol=self,
                )

                if self._disconnected:
                    return

                self._active_streams_sessions[event.stream_id] = stream
                app = self.config.loaded_application
                application_task = asyncio.get_event_loop().create_task(
                    stream.run_rcp(app=app)
                )  # create task
                self.server_state.application_tasks.add(application_task)  # store task in server state
                # remove task on done
                application_task.add_done_callback(self.server_state.application_tasks.discard)

        except (
            exceptions.InvalidPath,
            exceptions.InvalidAuthority,
            exceptions.InvalidScheme,
            exceptions.MethodNotAllowed,
            exceptions.DuplicatePseudoHeader,
            exceptions.InvalidPseudoHeader,
            exceptions.MalformedRequest,
            exceptions.H3MalformedMessage,
        ) as err:
            # reset stream on request malformed errors
            self._quic.reset_stream(stream_id=event.stream_id, error_code=ErrorCode.H3_MESSAGE_ERROR)
            self.transmit()
            protocol_logger.error(
                "Malformed HTTP/3 request received on stream %d", event.stream_id, exc_info=err
            )

        except exceptions.InvalidEvent as err:
            self._quic.reset_stream(stream_id=event.stream_id, error_code=ErrorCode.H3_INTERNAL_ERROR)
            self.transmit()
            protocol_logger.error(
                "Malformed HTTP/3 request received on stream %d", event.stream_id, exc_info=err
            )

        except exceptions.UnsupportedMethod as err:
            self._http.send_headers(
                stream_id=event.stream_id,
                headers=[
                    (b":status", b"405"),
                    # Standard requirement for a 405 response to send allowed methods
                    (b"allow", b"GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS, TRACE"),
                    (b"content-length", b"0"),
                ],
                end_stream=True,  # Smooth termination of response
            )
            self.transmit()
            protocol_logger.error(
                "WebTransport CONNECT request received on stream %d "
                "please enable WebTransport using --enable-webtransport",
                event.stream_id,
                exc_info=err,
            )

    def _schedule_disconnect(self, event: ConnectionTerminated):
        """Handle an HTTP/3 connection termination for all active streams."""
        try:
            if not isinstance(event, ConnectionTerminated):
                raise exceptions.InvalidEvent(event)
            self._disconnected = True

            for stream in self._active_streams_sessions.values():
                try:
                    if not isinstance(stream, HTTP3Stream):
                        continue

                    stream.close()
                except Exception as e:
                    protocol_logger.error(
                        "Exception while closing stream %d after connection termination",
                        stream.stream_id,
                        exc_info=e,
                    )
                    continue

            self.server_state.remove_connection(self)  # remove connection from server state
        except exceptions.InvalidEvent as err:
            protocol_logger.error("Failed to handle connection termination", exc_info=err)

    def _handle_stream_interrupt(self, event: StopSendingReceived | StreamReset):
        try:
            if not isinstance(event, (StreamReset, StopSendingReceived)):
                raise exceptions.InvalidEvent(event)

            stream = self._active_streams_sessions.get(event.stream_id)

            if stream is None:
                # Stream already closed/reset or unknown stream
                return

            if not isinstance(stream, HTTP3Stream):
                raise exceptions.InvalidStream(stream, HTTP3Stream)

            stream.close()
            stream._stream_reset = True

        except (exceptions.InvalidEvent, exceptions.InvalidStream) as err:
            protocol_logger.error(
                "Failed to handle HTTP/3 stream interrupt for stream %d", event.stream_id, exc_info=err
            )

    @staticmethod
    def header_list_size(headers):
        return sum(len(name) + len(value) + 32 for name, value in headers)

    def is_lowercase_header(name: bytes) -> bool:  # return True only if there is no uppercase character
        return not any(65 <= c <= 90 for c in name)


class H3Protocol(H3Connection):
    """
    A High-level HTTP/3 wrapper for aioquic's H3Connection.
    """

    def __init__(self, quic, enable_webtransport=False):
        super().__init__(quic, enable_webtransport)

    def _get_local_settings(self) -> dict[int, int]:
        """
        Override internal method to get local settings updated to latest draft version of webtransport
        """
        settings: dict[int, int] = {
            Setting.QPACK_MAX_TABLE_CAPACITY: self._max_table_capacity,
            Setting.QPACK_BLOCKED_STREAMS: self._blocked_streams,
            WebTransportSettings.SETTINGS_ENABLE_CONNECT_PROTOCOL: 1,
            Setting.DUMMY: 1,
        }
        if self._enable_webtransport:
            settings[WebTransportSettings.SETTINGS_H3_DATAGRAM] = 1
            settings[WebTransportSettings.SETTINGS_WT_ENABLED] = 1
            settings[WebTransportSettings.SETTINGS_WT_INITIAL_MAX_DATA] = 1 * 1024 * 1024
            settings[WebTransportSettings.SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI] = 5
            settings[WebTransportSettings.SETTINGS_WT_INITIAL_MAX_STREAMS_UNI] = 5
        return settings

    def _validate_settings(self, settings: dict[int, int]) -> None:
        """
        Override internal method to validate remote settings agaist latest draft version of webtransport
        """

        for setting in [
            WebTransportSettings.SETTINGS_ENABLE_CONNECT_PROTOCOL,
            WebTransportSettings.SETTINGS_WT_ENABLED,
            WebTransportSettings.SETTINGS_H3_DATAGRAM,
        ]:
            if setting in settings and settings[setting] not in (0, 1):
                raise SettingsError(f"{setting.name} setting must be 0 or 1")

        if (
            settings.get(WebTransportSettings.SETTINGS_H3_DATAGRAM) == 1
            and self._quic._remote_max_datagram_frame_size is None
        ):
            raise SettingsError("SETTINGS_H3_DATAGRAM requires max_datagram_frame_size transport parameter")

        if (
            settings.get(WebTransportSettings.SETTINGS_WT_ENABLED) == 1
            and settings.get(WebTransportSettings.SETTINGS_H3_DATAGRAM) != 1
        ):
            raise SettingsError("SETTINGS_WT_ENABLED requires SETTINGS_H3_DATAGRAM")

        if (
            settings.get(WebTransportSettings.SETTINGS_WT_ENABLED) == 1
            and settings.get(WebTransportSettings.SETTINGS_H3_DATAGRAM) == 1
            and settings.get(WebTransportSettings.SETTINGS_ENABLE_CONNECT_PROTOCOL) != 1
        ):
            raise SettingsError("SETTINGS_WT_ENABLED requires SETTINGS_ENABLE_CONNECT_PROTOCOL")
