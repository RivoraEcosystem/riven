from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.events import (
    ProtocolNegotiated,
    HandshakeCompleted,
    ConnectionTerminated,
    StreamReset,
    StopSendingReceived
)
from aioquic.h3.events import (
    HeadersReceived,
    DataReceived,
    H3Event
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
    HTTPSendEvents,
    HTTPRequestEvent,
    HTTPConnectionEventType,
    HTTPDisconnectEvent,
    RCPVersions,
    RequestMethod,
    HTTPScheme,
    HTTPResponseEventType,
    HTTPResponseStartEvent,
    HTTPResponseBodyEvent,
    HTTPResponseTrailersEvent,
    HTTPDisconnectEvent,
    RCPApplication
)
from ..server import Riven
from ._stream_utils import HTTPStreamContext , ConnectionInfo
import asyncio
from ..exceptions import exceptions
from typing import Any
from rcp.events import Headers
from collections.abc import Iterable
import logging
from typing import Literal

access_logger = logging.getLogger("riven.access")
protocol_logger = logging.getLogger("riven.protocol")

class RivenConnection(QuicConnectionProtocol):
    def __init__(self ,manager:Riven ,*args ,**kwargs):
        super().__init__(*args, **kwargs)
        self._manager = manager
        self._active_streams:dict[int,HTTPStreamContext] = dict()
        self._http = None
        self._manager.add_connection(self)
        self._disconnected = False
        


    def quic_event_received(self, event):
        if isinstance(event, ProtocolNegotiated):
            if event.alpn_protocol == H3_ALPN: # confirm HTTP3
                self._http = H3Connection(self._quic) # Upgrade to HTTP3

        elif isinstance(event,ConnectionTerminated):
            asyncio.create_task(self._schedule_disconnect(event))

        elif isinstance(event,StreamReset) or isinstance(event,StopSendingReceived):
            asyncio.create_task(self._handle_stream_interrupt(event))

        if self._http:
            for http_event in self._http.handle_event(event):
                asyncio.create_task(self._handle_h3_event(http_event))


    async def _handle_h3_event(self,event:H3Event):
        if isinstance(event,HeadersReceived):
            await self._create_stream(event=event,state=self._manager.state,extensions=self._manager.extensions)
        elif isinstance(event,DataReceived):
            await self._handle_data_received(event)

    async def _create_stream(
            self,
            event: HeadersReceived,
            state:dict[str,Any] | None = None,
            extensions:dict[str, dict[object, object]] | None = None
            ) -> None:
        """Parse pseudo headers of H3 Events and create HTTPScope , StreamContext and call application reject on max_header_size"""
        if not isinstance(event,HeadersReceived):
            raise exceptions.InvalidEvent(event)

        header_size = self.header_list_size(event.headers)

        if (self._manager.config.max_header_size > 0 # rejecting if headers exceed limit
            and header_size > self._manager.config.max_header_size
        ):
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

        # Iterate instead of using dict() to preserve duplicate HTTP headers
        # while validating pseudo-headers individually.
        for name, value in event.headers:

            if name == b":method":
                if method is not None:
                    raise exceptions.DuplicatePseudoHeader(":method")
                try:
                    method = RequestMethod(value.decode("ascii"))
                    # Reject CONNECT method
                    if method == RequestMethod.CONNECT:
                        raise exceptions.MethodNotAllowed("CONNECT")

                except ValueError:
                    raise exceptions.MethodNotAllowed(value.decode("ascii"))
                
            elif name == b":scheme":
                if scheme is not None:
                    raise exceptions.DuplicatePseudoHeader(":scheme")
                try:
                    scheme = HTTPScheme(value.decode("ascii"))
                except ValueError:
                    raise exceptions.InvalidScheme(value.decode("ascii"))
                
            elif name == b":authority":
                if authority is not None:
                    raise exceptions.DuplicatePseudoHeader(":authority")
                try:
                    authority = value.decode("ascii")
                except UnicodeDecodeError:
                    raise exceptions.InvalidAuthority(value)
                
            elif name == b":path":
                if raw_target is not None:
                    raise exceptions.DuplicatePseudoHeader(":path")
                if value != b"*" and not value.startswith(b"/"): # Reject invalid path values except / and *
                    raise exceptions.InvalidPath(value)
                raw_target = value

            elif name.startswith(b":"):
                raise exceptions.InvalidPseudoHeader(name.decode("ascii", "replace"))
            
            else:
                headers.append((name, value))

        if method is None:
            raise exceptions.MethodNotAllowed(None)

        if scheme is None:
            raise exceptions.InvalidScheme()

        if authority is None:
            raise exceptions.InvalidAuthority()

        if raw_target is None:
            raise exceptions.InvalidPath()

        raw_path, _, query_string = raw_target.partition(b"?") # convert raw target to raw path and query string with '?'

        try:
            path = raw_path.decode("utf-8")
        except UnicodeDecodeError:
            raise exceptions.InvalidPath(raw_path)
        
        client = self._transport.get_extra_info("peername") # client info from _transport 
        server = self._transport.get_extra_info("sockname") # server info from _transport

        http_scope: HTTPScope = {
            "type": ScopeType.HTTP,
            "rcp": {"version": RCPVersions.VERSION_1},
            "http_version": HTTPVersions.HTTP3,
            "method": method,
            "scheme": scheme,
            "authority": authority,
            "path": path,
            "raw_path": raw_path,
            "query_string": query_string,
            "root_path": self._manager.config.root_path,
            "headers": headers,
            "client": client,
            "server": server,
        }
        if state is not None:
            http_scope['state'] = state

        if extensions is not None:  
            http_scope['extensions'] = extensions

        connection = ConnectionInfo(
                stream_id=event.stream_id,
                server=server,
                client=client,
            )
        stream_context = HTTPStreamContext( # build http stream context
            connection=connection,
            stream_id=event.stream_id,
            method=method,
            scheme=scheme,
            http_version=HTTPVersions.HTTP3,
            protocol=self,
            path=path,
            max_queue_size=self._manager.config.max_queue_size
        )

        self._active_streams[event.stream_id] = stream_context
        try:
            task = await self._manager._start_rcp_application(http_scope,stream_context)
        except Exception:
            await stream_context.close()
            self._active_streams.pop(stream_context.stream_id,None)
            raise

        stream_context.task = task
        

    async def _handle_data_received(
        self,
        event: DataReceived
    ):
        """Parse DataReceived and generate HTTPRequestEvent and add to request queue for that stream and handle end streams to mark request end"""
        if not isinstance(event,DataReceived):
            raise exceptions.InvalidEvent(event)

        request_body:HTTPRequestEvent = {
            "type" : HTTPConnectionEventType.REQUEST,
            "body" : event.data,
            "more_body" : not event.stream_ended
        }

        context = self._active_streams.get(event.stream_id)

        if context is None:
            # Stream already closed/reset or unknown stream
            return

        if not isinstance(context,HTTPStreamContext):
            raise exceptions.InvalidStreamContext(context,HTTPStreamContext)

        await context._push_request(request_body)

        if event.stream_ended:
            await context._finish_request()

    async def _schedule_disconnect(
        self,
        event:ConnectionTerminated 
    ):
        """Handle an HTTP/3 connection termination by scheduling cleanup for all active streams."""
        if not isinstance(event,ConnectionTerminated):
            raise exceptions.InvalidEvent(event)

        active = [c for c in self._active_streams.values() if not c.is_closed]

        results = await asyncio.gather(
            *(self._close_stream(c) for c in active),
            return_exceptions=True,
        )

        for context, result in zip(active, results):

            if isinstance(result,Exception):
                ... # add logging logic after adding logging
                 
        self._disconnected = True
        self._manager._active_connections.pop(self._quic.host_cid,None) # remove connection manager

    async def _close_stream(
        self,
        context:HTTPStreamContext
    ):
        """Gracefully close an HTTP stream after a client disconnect."""
        disconnect_event:HTTPDisconnectEvent = {"type":HTTPConnectionEventType.DISCONNECT}
        if not context.request_complete:
            await context._push_request(disconnect_event)

        await asyncio.sleep(15) 
        await context.close()

        self._active_streams.pop(context.stream_id, None)

    async def _handle_stream_interrupt(self,event: StopSendingReceived | StreamReset):
        if not isinstance(event, (StreamReset, StopSendingReceived)):
            raise exceptions.InvalidEvent(event)

        context = self._active_streams.get(event.stream_id)
        
        if context is None:
            # Stream already closed/reset or unknown stream
            return
    
        if not isinstance(context,HTTPStreamContext):
            raise exceptions.InvalidStreamContext(context,HTTPStreamContext)

        await self._close_stream(context=context) # close stream by sending HTTPDisconnectEvent using _close_stream

    @staticmethod
    def header_list_size(headers):
        return sum(
            len(name) + len(value) + 32
            for name, value in headers
        )
    
    