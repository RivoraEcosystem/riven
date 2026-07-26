from rcp import (
    RCPApplication,
    RCPSendEvent,
    RCPReceiveEvent,
    HTTPVersions
)
from rcp.methods import RequestMethod
from rcp.scheme import HTTPScheme
import asyncio
from _connection_utils import ConnectionInfo
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ._socket import RivenConnection


class HTTPStreamContext:

    EOF = object() # EOF object for marking request's end 

    def __init__(
        self,
        connection:ConnectionInfo,
        stream_id:int,
        method:RequestMethod,
        scheme:HTTPScheme,
        http_version:HTTPVersions,
        protocol:RivenConnection
        ) -> None:
        self.stream_id = stream_id
        self.connection = connection
        self.method = method
        self.scheme = scheme
        self.http_version = http_version
        self._protocol = protocol

        self._request_queue:asyncio.Queue[RCPReceiveEvent] = asyncio.Queue(maxsize=10)

        self.request_forwarded = False
        self.closed = False
        self.task: asyncio.Task[None] | None = None # stores task of Rcp application

    async def _push_request(self, data:RCPReceiveEvent) -> None:
        "Add data to request queue buffer"

        await self._request_queue.put(data)

    async def _request_complete(self) -> None:
        "Add none at last to mark request data ending"

        await self._request_queue.put(self.EOF)
        await self._request_queue.shutdown(immediate=False)
        self.request_forwarded = True

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
        self.closed = True  

        if self.task is not None and not self.task.done(): # prevent canceling alraedy canclled or completed task
            self.task.cancel()

        await self._request_queue.shutdown(immediate=True) # shutdown queue immediately