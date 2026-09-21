from __future__ import annotations
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.events import (
    ProtocolNegotiated,
    ConnectionTerminated,
    StreamReset,
    StopSendingReceived,
)
from aioquic.h3.events import (
    HeadersReceived,
    DataReceived,
)
from aioquic.h3.connection import (
    H3_ALPN,
    H3Connection,
    ErrorCode
)
from rcp import (
    HTTPScope,
    ScopeType,
    HTTPVersions,
    RequestMethod,
    HTTPScheme,
    H3_FORBIDDEN_HEADERS
)
from ._stream_utils import HTTP3Stream , ConnectionInfo
import asyncio
from ..exceptions import exceptions
from typing import Any
import logging
from typing import TYPE_CHECKING
from _config import APPLICATION_INTERFACE_SPEC , RivenConfig
if TYPE_CHECKING:
    from ..server import RivenState


access_logger = logging.getLogger("riven.access")
protocol_logger = logging.getLogger("riven.protocol")

class RivenH3(QuicConnectionProtocol):
    def __init__(
            self,
            config:RivenConfig,
            app_state: dict[str, Any],
            server_state:RivenState,
            *args,
            **kwargs
        ):
        if not config.loaded:
            config.load()
        super().__init__(*args, **kwargs)
        self.connection_id = self._quic.host_cid
        self.config= config
        self.app_state = app_state
        self.server_state = server_state
        self._active_streams:dict[int,HTTP3Stream] = dict()
        self._http = None
        self._disconnected = False
        self.server_state.add_connection(self) # add connection to server state

    def quic_event_received(self, event):
        if isinstance(event, ProtocolNegotiated):
            if event.alpn_protocol == H3_ALPN: # confirm HTTP3
                self._http = H3Connection(self._quic) # Upgrade to HTTP3

        elif isinstance(event,ConnectionTerminated):
            self._schedule_disconnect(event)

        elif isinstance(event,StreamReset) or isinstance(event,StopSendingReceived):
            self._handle_stream_interrupt(event)

        if self._http:
            for http_event in self._http.handle_event(event):
                if isinstance(http_event,HeadersReceived):
                    self._create_stream(event=http_event)
                elif isinstance(http_event,DataReceived):
                    stream = self._active_streams.get(http_event.stream_id)

                    if not isinstance(stream,HTTP3Stream):
                        self._http._quic.reset_stream(stream_id=http_event.stream_id,error_code=ErrorCode.H3_INTERNAL_ERROR)
                        self.transmit()
                        protocol_logger.error("Failed to handle HTTP/3 DataReceived event for stream %d - Invalid Stream",http_event.stream_id)
                        return

                    if stream._closed:
                        self._http._quic.reset_stream(stream_id=http_event.stream_id,error_code=ErrorCode.H3_INTERNAL_ERROR)
                        self.transmit()
                        stream._stream_reset = True
                        protocol_logger.error("Failed to handle HTTP/3 DataReceived event for stream %d - Stream Closed By Application",http_event.stream_id)
                        return

                    if http_event.stream_ended:
                        stream.more_body = False
                        stream._request_complete = True

                    stream._body = http_event.data


    def _create_stream(
        self,
        event: HeadersReceived
        ) -> None:
        """Parse pseudo headers of H3 Events and create HTTPScope , Stream and Call the application for valid requests, but reject requests that exceed the configured maximum header size"""
        try:
            if not isinstance(event,HeadersReceived):
                raise exceptions.InvalidEvent(event)

            header_size = self.header_list_size(event.headers)

            if (self.config.max_header_size > 0 # rejecting if headers exceed limit
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


            # Iterate instead of using dict() to preserve duplicate HTTP headers
            # while validating pseudo-headers individually.
            for name, value in event.headers:

                if name != name.lower():
                    raise exceptions.H3MalformedMessage(f"Header name must be lowercase: {name!r}")

                if name.startswith(b":"):
                    if normal_header: # Check RFC 9114 4.1.2 https://datatracker.ietf.org/doc/html/rfc9114#section-4.1.2
                        raise exceptions.H3MalformedMessage("Request Malformed - Pseudo headers after normal headers")

                    if name == b":method":
                        if method is not None:
                            raise exceptions.H3MalformedMessage("Duplicate pseudo header :method")
                        try:
                            method = RequestMethod(value.decode("ascii")) # accept only Uppercase Methods 
                            # Reject CONNECT method
                            if method == RequestMethod.CONNECT:
                                raise exceptions.UnsupportedMethod("CONNECT")
    
                        except ValueError:
                            raise exceptions.MethodNotAllowed(value.decode("ascii"))
    
                    elif name == b":scheme":
                        if scheme is not None:
                            raise exceptions.H3MalformedMessage("Duplicate pseudo header :scheme")
                        try:
                            scheme = HTTPScheme(value.decode("ascii").lower())
                        except ValueError:
                            raise exceptions.InvalidScheme(value.decode("ascii"))
    
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

                    else:
                        raise exceptions.InvalidPseudoHeader(name.decode("ascii", "replace"))
                    
                else:

                    if name == b"te" and not value.lower() == b"trailers":
                        raise exceptions.H3MalformedMessage(f"Invalid value for header: 'te'")

                    if name in H3_FORBIDDEN_HEADERS:
                        raise exceptions.H3MalformedMessage(f"Forbidden headers in request: {name!r}")

                    if name == b"host": # store host header
                        host = value
                    
                    headers.append((name, value))
                    normal_header = True # normal headers started cant accept pseudo headers now

            if method is None:
                raise exceptions.MethodNotAllowed()
            
            if scheme in (HTTPScheme.HTTP, HTTPScheme.HTTPS):
                if authority is None or authority == b"":
                    raise exceptions.InvalidAuthority()
                
            if raw_target is None:
                raise exceptions.InvalidPath()

            if authority is not None and host is not None: # reject authority and host header if both exists but their value dont match check - https://datatracker.ietf.org/doc/html/rfc9114#section-4.3.1-4 
                if authority.encode("ascii") != host:
                    raise exceptions.H3MalformedMessage(
                        ":authority and Host do not match"
                    )

            if self.config.application_interface == "asgi" and not host:
                headers.append((b"host",bytes(authority))) # add host header if application_interface is set to ASGI
            
            # Target validation
            is_asterisk_form = (raw_target == b"*")
            
            if is_asterisk_form:
                if method != RequestMethod.OPTIONS:
                    raise exceptions.InvalidPath(raw_target)
            elif not (raw_target.startswith(b"/") or raw_target.startswith(b"http://") or raw_target.startswith(b"https://")):
                raise exceptions.InvalidPath(raw_target)

            # Split path and query string
            raw_path, _, query_string = raw_target.partition(b"?")

            try:
                # convert to empty string if its a astrik 
                path = "" if is_asterisk_form else raw_path.decode("utf-8")
            except UnicodeDecodeError:
                raise exceptions.InvalidPath(raw_path)

            client = self._transport.get_extra_info("peername") # client info from _transport 
            server = self._transport.get_extra_info("sockname") # server info from _transport

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
                "state" : self.app_state.copy()
            }

            if self.config.application_interface == "asgi":
                http_scope['asgi'] = APPLICATION_INTERFACE_SPEC['asgi']
            else:
                http_scope['rcp'] = APPLICATION_INTERFACE_SPEC['rcp'] 

            connection = ConnectionInfo(
                stream_id=event.stream_id,
                server=server,
                client=client,
            )

            stream_context = HTTP3Stream( # build http stream context
                connection=connection,
                stream_id=event.stream_id,
                scope=http_scope,
                protocol=self,
            )

            if self._disconnected:
                return
            
            self._active_streams[event.stream_id] = stream_context
            app = self.config.loaded_application
            application_task = asyncio.get_event_loop().create_task(stream_context.run_rcp(app=app)) # create task
            self.server_state.application_task = application_task # store task in server state
            application_task.add_done_callback(self.server_state.application_task.discard(application_task)) # remove task on done

        except (
            exceptions.InvalidPath,
            exceptions.InvalidAuthority,
            exceptions.InvalidScheme,
            exceptions.MethodNotAllowed,
            exceptions.DuplicatePseudoHeader,
            exceptions.InvalidPseudoHeader,
            exceptions.MalformedRequest,
            exceptions.H3MalformedMessage
            ) as err:
            self._http._quic.reset_stream(stream_id=event.stream_id,error_code=ErrorCode.H3_MESSAGE_ERROR) # reset stream on request malformed errors
            self.transmit()
            protocol_logger.error("Malformed HTTP/3 request received on stream %d",event.stream_id,exc_info=err)
            
        except exceptions.InvalidEvent as err:
            self._http._quic.reset_stream(stream_id=event.stream_id,error_code=ErrorCode.H3_INTERNAL_ERROR)
            self.transmit()
            protocol_logger.error("Malformed HTTP/3 request received on stream %d",event.stream_id,exc_info=err)

        except exceptions.UnsupportedMethod as err:
            self._http.send_headers(
                stream_id=event.stream_id,
                headers=[
                    (b":status", b"405"),
                    (b"allow", b"GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS, TRACE"), # Standard requirement for a 405 response
                    (b"content-length", b"0")
                ],
                end_stream=True # Smooth termination of response
            )
            self.transmit()
            protocol_logger.error("Unsupported HTTP/3 request received on stream %d",event.stream_id,exc_info=err)

    def _schedule_disconnect(
        self,
        event:ConnectionTerminated 
    ):
        """Handle an HTTP/3 connection termination for all active streams."""
        try:
            if not isinstance(event,ConnectionTerminated):
                raise exceptions.InvalidEvent(event)
            self._disconnected = True

            for stream in self._active_streams.values():
                try:
                    if not isinstance(stream,HTTP3Stream):
                        continue

                    stream.close()
                except Exception as e:
                    protocol_logger.error("Exception while closing stream %d after connection termination",stream.stream_id,exc_info=e)
                    continue

            self.server_state.remove_connection(self) # remove connection from server state
        except exceptions.InvalidEvent as err:
            protocol_logger.error("Failed to handle connection termination",exc_info=err)

    def _handle_stream_interrupt(self,event: StopSendingReceived | StreamReset):
        try:
            if not isinstance(event, (StreamReset, StopSendingReceived)):
                raise exceptions.InvalidEvent(event)

            context = self._active_streams.get(event.stream_id)

            if context is None:
                # Stream already closed/reset or unknown stream
                return

            if not isinstance(context,HTTP3Stream):
                raise exceptions.InvalidStream(context,HTTP3Stream)

            context.close()
            context._stream_reset = True
            
        except (exceptions.InvalidEvent, exceptions.InvalidStream) as err:
            protocol_logger.error("Failed to handle HTTP/3 stream interrupt for stream %d",event.stream_id,exc_info=err)

    @staticmethod
    def header_list_size(headers):
        return sum(
            len(name) + len(value) + 32
            for name, value in headers
        )