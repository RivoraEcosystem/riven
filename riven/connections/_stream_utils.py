from rcp import HTTPScope , ScopeType , HTTPVersions , RCPApplication
from rcp.methods import RequestMethod
from rcp.scheme import HTTPScheme
import asyncio
from typing import Optional
from _connection_utils import ConnectionInfo



class HTTPStreamContext:

    EOF = object() # EOF Object to mark data's end

    def __init__(self,connection_info:ConnectionInfo,stream_id:int,method:RequestMethod,scheme:HTTPScheme,http_version:HTTPVersions):
        self.stream_id = stream_id
        self.connection_info = connection_info
        self.method = method
        self.scheme = scheme
        self.http_version = http_version

        self._queue = asyncio.Queue(maxsize=10)

        self.response_started = False
        self.request_forwarded = False
        self.closed = False
        self.task: asyncio.Task | None = None # stores task of Rcp application

    async def add_chunk(self, data:bytes):
        "Add data to queue buffer"
        await self._queue.put(data)

    async def _finish(self):
        "Add none at last to mark request data ending"
        await self._queue.put(self.EOF)

    async def _read(self):
        "Read from queue buffer to free up queue"
        
        item = await self._queue.get()
    
        if item is self.EOF: # check if item is EOF then return None
            return None
        
        return item