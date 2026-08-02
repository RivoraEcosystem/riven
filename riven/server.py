from rcp import (
    RCPApplication,
    RCPReceiveCallable,
    RCPSendCallable,
    Scope,
    LifespanScope,
    HTTPRequestEvent,
    HTTPResponseDebugEvent,
    HTTPResponseStartEvent,
    HTTPResponseBodyEvent,
    HTTPResponseTrailersEvent,
    HTTPDisconnectEvent,
    LifespanStartupEvent,
    LifespanShutdownEvent,
    LifespanStartupCompleteEvent,
    LifespanStartupFailedEvent,
    LifespanShutdownCompleteEvent,
    LifespanShutdownFailedEvent,
)
from rcp.rcp import RCPVersions
from rcp.methods import RequestMethod
from rcp.scheme import HTTPScheme

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

from .lifespan import (
    LifespanContext,
    LifespanState
)

import asyncio
from ._config import RivenConfig
from typing import Any


type CONTEXT = HTTPStreamContext|LifespanContext

class Riven: # server connection manager 
    def __init__(
        self,
        app:RCPApplication,
        config:RivenConfig,
    ):
        self._application = app
        self.config = config
        self._active_connections:dict[bytes,RivenConnection] = dict()
        self.state:dict[str,Any] | None = None
        self.extensions:dict[str, dict[object, object]] | None = None

        self.lifespan: LifespanContext | None = None
        self._startup_future: asyncio.Future[None] | None = None
        self._shutdown_future: asyncio.Future[None] | None = None

    def add_connection(self,connection:RivenConnection) -> None:
        self._active_connections[connection.connection_id] = connection

    async def _start_rcp_application(self,scope:Scope,context:CONTEXT) -> asyncio.Task[None]:
        return asyncio.create_task(
        self._application(
            scope,
            context.receive,
            context.send,
        )
    )

    async def lifespan_startup(self) -> None:
        scope: LifespanScope = {
            "type": "lifespan",
            "rcp" : {"version":RCPVersions.VERSION_1}
        }

        if self.state is not None:
            scope["state"] = self.state

        self.lifespan = LifespanContext(self, scope)

        self._startup_future = asyncio.get_running_loop().create_future()

        self.lifespan.state = LifespanState.STARTING

        self.lifespan.task = await self._start_rcp_application(scope=scope,context=self.lifespan)

        await self.lifespan.startup()

        await self._startup_future

    async def lifespan_shutdown(self) -> None:
        if self.lifespan is None:
            return

        self._shutdown_future = asyncio.get_running_loop().create_future()

        self.lifespan.state = LifespanState.STOPPING

        await self.lifespan.shutdown()

        try:
            await self._shutdown_future
        finally:
            await self.lifespan.close()