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
    DataReceived
)
from aioquic.h3.connection import (
    H3_ALPN,
    H3Connection
)
from ..server import Riven
from ._stream_utils import StreamContext


class RivenConnection(QuicConnectionProtocol):
    def __init__(self ,manager:Riven ,*args ,**kwargs):
        super().__init__(*args, **kwargs)
        self._manager = manager
        self._active_streams:dict[int,StreamContext] = dict()
        self._http = None
        