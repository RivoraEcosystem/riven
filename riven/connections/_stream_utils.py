from rcp import (
    RCPApplication,
    RCPSendEvent,
    RCPReceiveEvent,
    HTTPVersions,
    HTTPScope
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
        self.stream_id = stream_id
        self.connection = connection
        self._protocol = protocol
        self.scope = scope

        self.method = method
        self.scheme = scheme
        self.http_version = http_version
        self._path = path

        self._closed = False

        self._request_queue:asyncio.Queue[RCPReceiveEvent] = asyncio.Queue(maxsize=max_queue_size)
        self._request_complete = False

        self._response_status_code:int|None = None
        self._response_started:bool = False        
        self._response_body_sent: bool = False
        self._response_complete:bool = False

        self._stream_reset: bool = False

        self.task: asyncio.Task[None] | None = None # stores task of Rcp application
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

    async def send(self,event:RCPSendEvent):
        "Send function for RCPApplication which parses events and handle that event forward data accordingly"

        return await self._protocol.handle_send_event(self.stream_id,event)

    async def receive(self) -> RCPReceiveEvent | None:
        "Receive function for RCPApplication which forward a event from request queue"

        return await self._pop_request()

    async def close(self):
        if self._closed:
            return

        self._closed = True

        if self.task is not None and not self.task.done(): # prevent canceling alraedy canclled or completed task
            self.task.cancel()

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
    
        except BaseException as exc:
    
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

    async def reset_stream(self,stream_id:int,error:ErrorCode = ErrorCode.H3_INTERNAL_ERROR):
        """Reset HTTP3 stream with H3_INTERNAL_ERROR"""
        self._protocol._quic.reset_stream(stream_id=stream_id,error_code=error)
        self._protocol.transmit()

    async def handle_close(self):
        """Close StreamContext and remove from active stream"""
        await self.close()
        self._protocol._active_streams.pop(self.stream_id, None)        