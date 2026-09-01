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
    RCPApplication,
    RCPReceiveEvent,
    RCPSendEvent
)
from rcp.methods import RequestMethod
from rcp.scheme import HTTPScheme
import asyncio
from _connection_utils import ConnectionInfo
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ._socket import RivenConnection
from dataclasses import dataclass
import logging
from aioquic.h3.connection import (
    ErrorCode
)
from ..exceptions import exceptions
from collections.abc import Iterable
from typing import Literal

access_logger = logging.getLogger("riven.access")
protocol_logger = logging.getLogger("riven.protocol")

class HTTPStreamContext:

    EOF = object() # EOF object for marking request's end 

    def __init__(
        self,
        connection:ConnectionInfo,
        stream_id:int,
        scope:HTTPScope,
        method:RequestMethod,
        scheme:HTTPScheme,
        http_version:HTTPVersions,
        protocol:RivenConnection,
        path:str,
        max_queue_size:int|None=0
        ) -> None:

        self.connection = connection
        self.stream_id = stream_id
        
        self.scope = scope
        self.method = method
        self.scheme = scheme
        self.http_version = http_version
        self._protocol = protocol
        self._path = path
        self._request_queue:asyncio.Queue[RCPReceiveEvent] = asyncio.Queue(maxsize=max_queue_size)

        self.task:asyncio.Task = None

        self._closed = False

        self._request_complete = False

        self._response_status_code:int|None = None
        self._response_started:bool = False        
        self._response_body_sent: bool = False
        self._response_complete:bool = False

        self._stream_reset: bool = False
        
        self.trailers_enabled:bool = False # Whether the application declared that trailers will be sent

    async def _push_request(self, data:RCPReceiveEvent) -> None:
        "Add data to request queue buffer"

        await self._request_queue.put(data)

    async def _finish_request(self) -> None:
        "Add none at last to mark request data ending"

        await self._request_queue.put(self.EOF)
        self._request_queue.shutdown(immediate=False)
        self._request_complete = True

    async def _pop_request(self) -> RCPReceiveEvent | None:
        "Read from request queue buffer to free up queue"
        
        item = await self._request_queue.get()
    
        if item is self.EOF: # check if item is EOF then return None
            return None
        
        return item

    async def receive(self) -> RCPReceiveEvent | None:
        "Receive function for RCPApplication which forward a event from request queue"

        return await self._pop_request()

    async def close(self):
        if self._closed:
            return

        self._closed = True
        self._request_queue.shutdown(immediate=True) # shutdown queue immediately

    @property
    def is_closed(self):
        return self._closed

    @property
    def request_complete(self):
        return self._request_complete

    @property
    def response_body_sent(self):
        return self._response_body_sent

    @property
    def stream_reset(self):
        return self._stream_reset

    # RCP Exception Wrapper
    async def run_rcp(self,app:RCPApplication) -> None:
        if self._protocol._disconnected: # return on Disconnected connections
            return
    
        if self.is_closed: # stream closed already
            return
    
        try:
            result = await app(self.scope, self.receive, self.send)
    
        except Exception as exc:
    
            msg = "Exception in RCP application\n"
            protocol_logger.error(msg, exc_info=exc)
            if not self._response_started:
                await self.send_500_response(stream_id=self.stream_id,self=self)
                await self.handle_close(self=self)
            else:
                await self.reset_stream(stream_id=self.stream_id)
                await self.handle_close(self=self)
    
        else:
            if result is not None:
                msg = f"RCP callable should return None, but returned {result}."
                protocol_logger.error(msg)
                await self.reset_stream(stream_id=self.stream_id)
                self._stream_reset = True
                await self.handle_close(self=self)
            elif not self._response_started and not self.is_closed:
                msg = "RCP callable returned without starting response."
                protocol_logger.error(msg)
                await self.send_500_response(stream_id=self.stream_id,self=self)
                await self.handle_close(self=self)
            elif not self._response_complete and not self.is_closed:
                msg = "RCP callable returned without completing response."
                protocol_logger.error(msg)
                await self.reset_stream(stream_id=self.stream_id)
                await self.handle_close(self=self)
    
        finally:
            ...

    async def reset_stream(self,error:ErrorCode = ErrorCode.H3_INTERNAL_ERROR):
        """Reset HTTP3 stream with H3_INTERNAL_ERROR"""
        self._protocol._quic.reset_stream(stream_id=self.stream_id,error_code=error)
        self._protocol.transmit()

    async def handle_close(self):
        """Close StreamContext and remove from active stream"""
        await self.close()
        self._protocol._active_streams.pop(self.stream_id, None)   

    async def send_500_response(self,stream_id:int) -> None: # send 500 Internal Server Error response to client
    
        access_logger.error(
            "",
            extra={
                "client_addr": self.connection.client,
                "method": self.method,
                "path": self._path,
                "status_code": 500,
            },
        )

        self._protocol._http.send_headers( # 500 error headers
            stream_id=stream_id,
            headers=[
                (b":status", b"500"),
                (b"content-type", b"text/plain"),
                (b"content-length", b"21"),
            ],
            end_stream=False,
        )
        self._protocol._http.send_data( # 500 error body
            stream_id=stream_id,
            data=b"Internal Server Error",
            end_stream=True,
        )
        self._protocol.transmit()

        return     

    async def send(self,event:RCPSendEvent):
        "Send function for RCPApplication which parses events and handle that event forward data accordingly"

        if self._protocol._disconnected:
            return

        if self._response_complete: # response already completed
            raise RuntimeError(f"RCP Violation - Unexpected RCP event response already completed.")

        if self.is_closed:
            raise RuntimeError(f"RCP Violation - Unexpected RCP Event stream closed.")

        if event["type"] == HTTPResponseEventType.START:

            event:HTTPResponseStartEvent = event

            if self._response_started:
                raise RuntimeError(f"RCP Violation - Headers already sent")
            
            status_code = event.get("status")
            self.trailers_enabled = bool(event.get("trailers", False))
            headers = event.get("headers")


            self.validate_rcp_response_start_fields(
                event=event,
                status_code=status_code,
                stream_id=self.stream_id,
                headers=headers
            )

            pseudo_headers = self.construct_pseudo_headers(status_code) # construct pseduo headers for HTTP3 response

            new_headers = list(headers)
            new_headers.extend(pseudo_headers)

            
            self._protocol._http.send_headers(stream_id=self.stream_id,headers=new_headers,end_stream=False)

            self._response_status_code = status_code
            self._response_started = True

            self._protocol.transmit()

            return


        elif event["type"] == HTTPResponseEventType.BODY:

            event:HTTPResponseBodyEvent = event

            if not self._response_started: # Body arrived before headers
                raise RuntimeError("RCP Violation - Response not started")

            body:bytes = event.get("body")
            more_body:bool = bool(event.get('more_body',False)) # assume no more body as False as more body is optional

            if not isinstance(body,bytes):
                raise exceptions.InvalidEventField(
                    field="body",
                    got=type(event['body']),
                    expected=bytes,
                )
            
            end_stream = not more_body and not self.trailers_enabled # end stream only when there is no more body and there are no trailers
            self._protocol._http.send_data(stream_id=self.stream_id,data=body,end_stream=end_stream)
            self._protocol.transmit()

            if not more_body:
                self._response_body_sent = True
            self._response_complete = end_stream
                    
        elif event["type"] == HTTPResponseEventType.TRAILERS:
            ...
        elif event["type"] == HTTPConnectionEventType.DISCONNECT:
            ...
        else:
            raise RuntimeError(f"RCP Violation - Unexpected '{event["type"]}' event sent")

    def construct_pseudo_headers(self,status_code: int) -> list[tuple[bytes, bytes]]:
            return [
                (b":status", str(status_code).encode("ascii"))
            ]
            
    def check_status(self, code: int) -> bool: # check if status code is in range of valid HTTP codes 100 to 599
        return 100 <= code <= 599

    def validate_rcp_response_start_fields(self,event:HTTPResponseStartEvent,)-> Literal[True]:

        """Check and validate rcp event fields"""
        
        if not isinstance(event['status'],int): # Status code validation 
            raise exceptions.InvalidEventField(
                field="status",
                got=type(event["status"]),
                expected=int,
            )
        
        if not self.check_status(event['status']):
            raise exceptions.InvalidStatusCode(f"Invalid HTTP Status code: {event['status']}")
        
        if event['headers'] is None:
            raise exceptions.InvalidEventField(
                field="headers",
                got=type(event['headers']),
                expected=Iterable
            )
        
        for header in event['headers']:

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