from rcp import HTTPScope , ScopeType , HTTPVersions , RCPApplication , RCPSendEvent , RCPReceiveEvent
from rcp.methods import RequestMethod
from rcp.scheme import HTTPScheme
import asyncio
from typing import Optional
from _connection_utils import ConnectionInfo



class HTTPStreamContext:

    _REQUEST_EOF = object() # EOF object for marking request's end 
    _RESPONSE_EOF = object() # EOF object for marking response's end

    def __init__(self,connection:ConnectionInfo,stream_id:int,method:RequestMethod,scheme:HTTPScheme,http_version:HTTPVersions):
        self.stream_id = stream_id
        self.connection = connection
        self.method = method
        self.scheme = scheme
        self.http_version = http_version

        self._request_queue:asyncio.Queue[RCPReceiveEvent] = asyncio.Queue(maxsize=10)
        self._response_queue:asyncio.Queue[RCPSendEvent] = asyncio.Queue(maxsize=10)

        self.response_started = False
        self.request_forwarded = False
        self.closed = False
        self.task: asyncio.Task | None = None # stores task of Rcp application

    async def _push_request(self, data:RCPReceiveEvent) -> None:
        "Add data to request queue buffer"
        await self._request_queue.put(data)

    async def _finish_request(self) -> None:
        "Add none at last to mark request data ending"
        await self._request_queue.put(self._REQUEST_EOF)
        await self._response_queue.shutdown(immediate=False)

    async def _pop_request(self) -> RCPReceiveEvent | None:
        "Read from request queue buffer to free up queue"
        
        item = await self._request_queue.get()
    
        if item is self._REQUEST_EOF: # check if item is EOF then return None
            return None
        
        return item

    async def _push_response(self, data:RCPSendEvent) -> None:
        "Add data to response queue buffer"
        await self._response_queue.put(data)
    
    async def _finish_response(self) -> None:
        "Add none at last to mark response data ending"
        await self._response_queue.put(self._RESPONSE_EOF)
        await self._response_queue.shutdown(immediate=False)

    async def _pop_response(self) -> RCPSendEvent | None:
        "Read from response queue buffer to free up queue"
        
        item = await self._response_queue.get()
    
        if item is self._RESPONSE_EOF: # check if item is EOF then return None
            return None
        
        return item

    async def send(self,event:RCPSendEvent):
        "Send function for RCPApplication which parses events and handle that event forward data accordingly"
        pass

    async def receive(self) -> RCPReceiveEvent | None:
        "Receive function for RCPApplication which forward a event from request queue"
        return await self._pop_request()