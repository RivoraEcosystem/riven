from __future__ import annotations
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
from ._connection_utils import ConnectionInfo
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ._protocol import RivenH3
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

class HTTP3Stream:

    _CLOSED = 1 << 0
    _REQUEST_COMPLETE = 1 << 1
    _RESPONSE_STARTED = 1 << 2
    _RESPONSE_BODY_SENT = 1 << 3
    _RESPONSE_COMPLETE = 1 << 4
    _STREAM_RESET = 1 << 5
    _TRAILERS_ENABLED = 1 << 6

    __slots__ = (
        "connection",
        "stream_id",
        "scope",
        "_protocol",
        "_queue",
        "_flags",
        "_push_status",
        "_push_event",
        "task",
        "_response_status_code",
    )

    def __init__(
        self,
        connection:ConnectionInfo,
        stream_id:int,
        scope:HTTPScope,
        protocol:RivenH3,
        max_queue_size:int|None=0
        ) -> None:
        self.connection = connection
        self.stream_id = stream_id
        
        self.scope = scope
        self._protocol = protocol
        self._queue:asyncio.Queue[RCPReceiveEvent] = asyncio.Queue(maxsize=max_queue_size)
    
        self._flags:int = 0
    
        self._push_status = asyncio.Event() # event for push
        self._push_status.set()
        self._push_event:RCPReceiveEvent|None = None
    
        self.task:asyncio.Task = None
    
        self._response_status_code:int|None = None

    @property
    def _closed(self) -> bool:
        return self._get_flag(self._CLOSED)

    @_closed.setter
    def _closed(self,value:bool) -> None:
        self._set_flag(self._CLOSED, value)

    @property
    def _request_complete(self):
        return self._get_flag(self._REQUEST_COMPLETE)

    @_request_complete.setter
    def _request_complete(self,value:bool) -> None:
        self._set_flag(self._REQUEST_COMPLETE, value)    

    @property
    def _response_started(self):
        return self._get_flag(self._RESPONSE_STARTED)

    @_response_started.setter
    def _response_started(self,value:bool) -> None:
        self._set_flag(self._RESPONSE_STARTED, value)    

    @property
    def _response_body_sent(self):
        return self._get_flag(self._RESPONSE_BODY_SENT)

    @_response_body_sent.setter
    def _response_body_sent(self,value:bool) -> None:
        self._set_flag(self._RESPONSE_BODY_SENT, value)    

    @property
    def _response_complete(self):
        return self._get_flag(self._RESPONSE_COMPLETE)

    @_response_complete.setter
    def _response_complete(self,value:bool) -> None:
        self._set_flag(self._RESPONSE_COMPLETE, value)    

    @property
    def _stream_reset(self):
        return self._get_flag(self._STREAM_RESET)

    @_stream_reset.setter
    def _stream_reset(self,value:bool) -> None:
        self._set_flag(self._STREAM_RESET, value)    

    @property
    def trailers_enabled(self):
        return self._get_flag(self._TRAILERS_ENABLED)

    @trailers_enabled.setter
    def trailers_enabled(self,value:bool) -> None:
        self._set_flag(self._TRAILERS_ENABLED, value)

    def _get_flag(self,mask:int) -> bool:
            return bool(self._flags & mask)
    
    def _set_flag(self, mask: int, value: bool) -> None:
        if value:
            self._flags |= mask
        else:
            self._flags &= ~mask

    async def _push_to_queue(self, data:RCPReceiveEvent) -> None:
        "Add data to queue buffer"

        await self._queue.put(data)

    async def _pop_from_queue(self) -> RCPReceiveEvent | None:
        "Read from request queue buffer to free up queue"
        
        item = await self._queue.get()
    
        return item

    async def receive(self) -> RCPReceiveEvent | None:
        "Receive function for RCPApplication which forward a event from request queue"

        if self._push_event is not None:
            return await self.get_push()

        return await self._pop_from_queue()

    async def push_event(self,event:RCPReceiveEvent) -> None:
        if event is None:
            return

        if self._push_event is not None:
            await self._push_status.wait() # wait for existing event 

        self._push_event = event

        self._push_status.clear() # reset event to make futher setters wait

    async def get_push(self) -> RCPReceiveEvent:
        if self._push_event is None:
            raise RuntimeError("No Push event")

        event = self._push_event
        self._push_event = None

        self._push_status.set()
        return event


    async def close(self):
        if self._closed:
            return

        self._closed = True
        self._queue.shutdown(immediate=True) # shutdown queue immediately

    # RCP Exception Wrapper
    async def run_rcp(self,app:RCPApplication) -> None:
        if self._protocol._disconnected: # return on Disconnected connections
            return
    
        if self._closed: # stream closed already
            return
    
        try:
            result = await app(self.scope, self.receive, self.send)
    
        except Exception as exc:
    
            msg = "Exception in RCP application\n"
            protocol_logger.error(msg, exc_info=exc)
            if not self._response_started:
                await self.send_500_response(stream_id=self.stream_id,self=self)
            else:
                await self.reset_stream()
    
        else:
            if result is not None:
                msg = f"RCP callable should return None, but returned {result}."
                protocol_logger.error(msg)
                await self.reset_stream()
            elif not self._response_started and not self._closed:
                msg = "RCP callable returned without starting response."
                protocol_logger.error(msg)
                await self.send_500_response(stream_id=self.stream_id,self=self)
            elif not self._response_complete and not self._closed:
                msg = "RCP callable returned without completing response."
                protocol_logger.error(msg)
                await self.reset_stream()
    
        finally:
            if not self._response_complete and not self._stream_reset:
                await self.reset_stream()
            await self.handle_close() # close stream at last
            

    async def reset_stream(self,error:ErrorCode = ErrorCode.H3_INTERNAL_ERROR):
        """Reset HTTP3 stream with H3_INTERNAL_ERROR"""
        if self._stream_reset:
            return
        
        self._protocol._quic.reset_stream(stream_id=self.stream_id,error_code=error)
        self._protocol.transmit()
        self._stream_reset = True

    async def handle_close(self):
        """Close StreamContext and remove from active stream"""
        await self.close()
        self._protocol._active_streams.pop(self.stream_id, None)   

    async def send_500_response(self,stream_id:int) -> None: # send 500 Internal Server Error response to client

        access_logger.error(
            '%s - "%s %s HTTP/%s" %d',
            self.get_client_addr(self.scope),
            self.scope['method'],
            self.get_full_path(self.scope),
            self.scope['http_version'],
            500,
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

        if self._closed:
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

    def get_client_addr(self, scope: HTTPScope) -> str:
        client = scope.get("client")
        if not client:
            return ""
    
        return "%s:%d" % client
    
    
    def get_full_path(self, scope: HTTPScope) -> str:
        raw_path = scope.get("raw_path")
        if raw_path:
            return raw_path.decode("utf-8")
    
        path = scope.get("path", "")
        query_string = scope.get("query_string", b"")
    
        if query_string:
            return f"{path}?{query_string.decode('utf-8')}"
    
        return path