from rcp import (
    RCPApplication,
    RCPReceiveCallable,
    RCPSendCallable,
    Scope
)
from connections import (
    RivenConnection,
    ConnectionInfo,
    HTTPStreamContext,
    EventHandler
)
from .exceptions.exceptions import (
    RivenException,
    InvalidEvent,
    DuplicatePseudoHeader,
    InvalidPseudoHeader,
    MethodNotAllowed,
    InvalidScheme,
    InvalidAuthority,
    InvalidPath,
    InvalidStreamContext
)
import asyncio
from ._config import RivenConfig
from typing import Any

class Riven: # server connection manager 
    def __init__(
        self,
        app:RCPApplication,
        config:RivenConfig,
    ):
        self._application = app
        self.config = config
        self._active_connections:dict[bytes,RivenConnection] = dict()
        self.root_path = ""
        self.state:dict[str,Any] = None
        self.extensions:dict[str, dict[object, object]] | None = None

    def add_connection(self,connection:RivenConnection) -> None:
        self._active_connections[connection._quic.host_cid] = connection

    async def _start_rcp_application(self,scope:Scope,context:HTTPStreamContext) -> asyncio.Task[None]:
        return asyncio.create_task(
        self._application(
            scope,
            context.receive,
            context.send,
        )
    )