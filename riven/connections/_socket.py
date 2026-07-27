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
    H3Connection
)
from rcp import (
    HTTPScope,
    ScopeType,
    HTTPVersions
)
from rcp.rcp import RCPVersions
from rcp.methods import RequestMethod

from ..server import Riven
from ._stream_utils import HTTPStreamContext
import asyncio

class RivenConnection(QuicConnectionProtocol):
    def __init__(self ,manager:Riven ,*args ,**kwargs):
        super().__init__(*args, **kwargs)
        self._manager = manager
        self._active_streams:dict[int,HTTPStreamContext] = dict()
        self._http = None
        self._manager.add_connection(self)


    def quic_event_received(self, event):
        if isinstance(event, ProtocolNegotiated):
            if event.alpn_protocol == H3_ALPN: # confirm HTTP3
                self._http = H3Connection(self._quic) # Upgrade to HTTP3

        elif isinstance(event,ConnectionTerminated):
            asyncio.create_task(self._handle_disconnect(event))

        if self._http:
            for http_event in self._http.handle_event(event):
                asyncio.create_task(self.handle_h3_event(http_event))


    async def handle_h3_event(self,event:H3Event):
        if isinstance(event,HeadersReceived):
            await self.handle_headers(event)
        elif isinstance(event,DataReceived):
            await self.handle_data_received(event)