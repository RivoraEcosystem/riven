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
from rcp.scheme import HTTPScheme
from ..server import Riven
from ..exceptions import exceptions
from typing import Any


class EventHandler:
    def __init__(self,manager:Riven):
        self._manager = manager
