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

    async def handle_send_event(self,stream_id,event:HTTPSendEvents):
        if self._disconnected:
            return

        context = self._active_streams.get(stream_id)

        if not isinstance(context,HTTPStreamContext): # invalid stream context
            client = self._transport.get_extra_info("peername") # get client info
            protocol_logger.error(f"{client[0]}:{client[1]} - Invalid Stream context for stream id {stream_id}")
            raise exceptions.InvalidStreamContext(context,HTTPStreamContext)

        if context._response_complete: # response already completed
            await self.handle_close(context=context) # close stream
            if not context.stream_reset:
                await self.reset_stream(stream_id=stream_id) # reset stream
                context._stream_reset = True
            access_logger.error(
                "",
                extra={
                    "client_addr": context.connection.client,
                    "method": context.method,
                    "path": context._path,
                    "status_code": "",
                },
            )
            raise RuntimeError(f"Unexpected RCP Event after response already completed.")

        if context.is_closed:
            if not context.stream_reset:
                await self.reset_stream(stream_id=stream_id) # reset stream
                context._stream_reset = True
            access_logger.error(
                "",
                extra={
                    "client_addr": context.connection.client,
                    "method": context.method,
                    "path": context._path,
                    "status_code": "",
                },
            )
            raise RuntimeError(f"Unexpected RCP Event after stream closed.")

        if event["type"] == HTTPResponseEventType.START:

            event:HTTPResponseStartEvent = event

            if context._response_started:
                if not context.stream_reset:
                    await self.reset_stream(stream_id=stream_id)
                    context._stream_reset = True
                client = self._transport.get_extra_info("peername") # get client info
                protocol_logger.error(f"Failed to send Response Start to Client - {client[0]}:{client[1]} Response already started")
                raise RuntimeError(f"Unexpected HTTPResponseStartEvent after response already started")
            
            status_code = event.get("status")
            context.trailers_enabled = bool(event.get("trailers", False))
            headers = event.get("headers")


            self.validate_rcp_response_start_fields(
                event=event,
                status_code=status_code,
                stream_id=stream_id,
                context=context,
                headers=headers
            )

            pseudo_headers = self.construct_pseudo_headers(context._response_status_code) # construct pseduo headers for HTTP3 response

            new_headers = list(headers)
            new_headers.extend(pseudo_headers)

            
            self._http.send_headers(stream_id=context.stream_id,headers=new_headers,end_stream=False)

            context._response_status_code = status_code
            context._response_started = True

            self.transmit()

            return


        elif event["type"] == HTTPResponseEventType.BODY:

            event:HTTPResponseBodyEvent = event

            if not context._response_started: # Body arrived before headers
                if not context.stream_reset:
                    await self.reset_stream(stream_id=stream_id)
                    context._stream_reset = True
                client = self._transport.get_extra_info("peername") # get client info
                protocol_logger.error(f"Failed to send Response Body to Client - {client[0]}:{client[1]} Body arrived before headers")
                raise RuntimeError(f"Unexpected HTTPResponseBodyEvent before response HTTPResponseStartEvent ")

            body:bytes = event.get("body")
            more_body:bool = bool(event.get('more_body',False)) # assume no more body as False as more body is optional

            if not isinstance(body,bytes):
                raise exceptions.InvalidEventField(
                    field="body",
                    got=type(event['body']),
                    expected=bytes,
                )
            
            end_stream = not more_body and not context.trailers_enabled # end stream only when there is no more body and there are no trailers
            self._http.send_data(stream_id=context.stream_id,data=body,end_stream=end_stream)
            self.transmit()

            if not more_body:
                context._response_body_sent = True
            context._response_complete = end_stream
                    
        elif event["type"] == HTTPResponseEventType.TRAILERS:
            ...
        elif event["type"] == HTTPConnectionEventType.DISCONNECT:
            ...
        else:
            raise RuntimeError(f"Unexpected RCP message '{event["type"]}' sent, after response already completed.")
        
    
    def construct_pseudo_headers(self,status_code: int) -> list[tuple[bytes, bytes]]:
        return [
            (b":status", str(status_code).encode("ascii"))
        ]

    async def send_500_response(self,stream_id:int,context:HTTPStreamContext) -> None: # send 500 Internal Server Error response to client

        if not isinstance(context,HTTPStreamContext): # invalid stream context
            raise exceptions.InvalidStreamContext(context,HTTPStreamContext)

        access_logger.error(
            "",
            extra={
                "client_addr": context.connection.client,
                "method": context.method,
                "path": context._path,
                "status_code": 500,
            },
        )

        self._http.send_headers( # 500 error headers
            stream_id=stream_id,
            headers=[
                (b":status", b"500"),
                (b"content-type", b"text/plain"),
                (b"content-length", b"21"),
            ],
            end_stream=False,
        )
        self._http.send_data( # 500 error body
            stream_id=stream_id,
            data=b"Internal Server Error",
            end_stream=True,
        )
        self.transmit()

        return

        
    def check_status(self, code: int) -> bool: # check if status code is in range of valid HTTP codes 100 to 599
        return 100 <= code <= 599

    def validate_rcp_response_start_fields(
            self,
            event:HTTPSendEvents,
            status_code:int,
            context:HTTPStreamContext,
            headers:Iterable
        )-> Literal[True]:

        """Check and validate rcp event fields"""

        if not isinstance(context,HTTPStreamContext): # invalid stream context
            raise exceptions.InvalidStreamContext(context,HTTPStreamContext)
        
        if not isinstance(status_code,int): # Status code validation 
            raise exceptions.InvalidEventField(
                field="status",
                got=type(event["status"]),
                expected=int,
            )
        
        if not self.check_status(status_code):
            raise exceptions.InvalidStatusCode(f"Invalid HTTP Status code: {status_code}")
        
        if headers is None:
            raise exceptions.InvalidEventField(
                field="headers",
                got=type(headers),
                expected=Iterable
            )
        
        for header in headers:

            if not isinstance(header, tuple) or len(header) != 2:
                raise exceptions.InvalidEventField(
                    field="headers",
                    got=type(header),
                    expected=tuple,
                )
            
            name, value = header

            if not isinstance(name, bytes):
                raise exceptions.InvalidEventField(
                    field="header name",
                    got=type(name),
                    expected=bytes,
                )
            
            if not isinstance(value, bytes):
                raise exceptions.InvalidEventField(
                    field="header value",
                    got=type(value),
                    expected=bytes,
                )
            
        return True

    async def reset_stream(self,stream_id:int,error:ErrorCode = ErrorCode.H3_INTERNAL_ERROR):
        """Reset HTTP3 stream with H3_INTERNAL_ERROR"""
        self._quic.reset_stream(stream_id=stream_id,error_code=error)
        self.transmit()

    async def handle_close(self,context:HTTPStreamContext):
        """Close StreamContext and remove from active stream"""
        await context.close()
        self._active_streams.pop(context.stream_id, None)    