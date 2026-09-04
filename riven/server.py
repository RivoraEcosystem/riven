from rcp import (
    RCPApplication,
    RCPReceiveCallable,
    RCPSendCallable,
    Scope,
    LifespanScope,
    LifespanEventType,
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

from .connections import (
    RivenConnection,
    ConnectionInfo,
    HTTPStream,
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
    InvalidStreamContext,
    InvalidLifespanState,
    LifespanAlreadyCompleted,
    LifespanNotStarted
)

from .lifespan import (
    LifeSpan,
    LifespanState
)

import asyncio
from ._config import (
    RivenConfig,
    STARTUP_SHUTDOWN_FAILURE
    )
from typing import Any
import sys


type LifespanSendEvent = LifespanStartupCompleteEvent|LifespanStartupFailedEvent|LifespanShutdownCompleteEvent|LifespanShutdownFailedEvent

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

        self.lifespan: LifeSpan | None = None
        self.root_path = config.root_path

    def add_connection(self,connection:RivenConnection) -> None:
        self._active_connections[connection.connection_id] = connection

    async def start_lifespan(self):
        lifespan = LifeSpan(
            manager=self
        )
        self.lifespan = lifespan
        self.lifespan.task = asyncio.get_running_loop().create_task(
            self.lifespan.main(
                app=self._application,
                state=self.state,
                extensions=self.extensions
            )
        ) # start lifespan
        await self.lifespan.startup() # send startup event
        if self.lifespan.should_exit:
            sys.exit(STARTUP_SHUTDOWN_FAILURE)

    async def shutdown_lifespan(self): # send shutdown event
        if self.lifespan is None or self.lifespan.closed:
            return
        
        await self.lifespan.shutdown()

        if self.lifespan.should_exit:
            sys.exit(STARTUP_SHUTDOWN_FAILURE)