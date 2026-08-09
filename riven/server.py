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

from connections import (
    RivenConnection,
    ConnectionInfo,
    HTTPStreamContext,
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
    LifespanContext,
    LifespanState
)

import asyncio
from ._config import RivenConfig
from typing import Any


type CONTEXT = HTTPStreamContext|LifespanContext
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

        self.lifespan: LifespanContext | None = None
        self._startup_future: asyncio.Future[None] | None = None
        self._shutdown_future: asyncio.Future[None] | None = None
        self.root_path = config.root_path if not config.root_path else ""

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
        if self.extensions is not None:
            scope["extensions"] = self.extensions

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

    async def handle_lifespan_event(
        self,
        context: LifespanContext,
        event: LifespanSendEvent,
    ) -> None:
        match event["type"]:

            case LifespanEventType.STARTUP_COMPLETE:

                if context.state is not LifespanState.STARTING:
                    raise InvalidLifespanState(
                        context.state,
                        event["type"],
                    )

                if self._startup_future is None:
                    raise LifespanNotStarted("startup")

                if self._startup_future.done():
                    raise LifespanAlreadyCompleted("startup")

                context.state = LifespanState.STARTED
                self._startup_future.set_result(None)

            case LifespanEventType.STARTUP_FAILED:

                if context.state is not LifespanState.STARTING:
                    raise InvalidLifespanState(
                        context.state,
                        event["type"],
                    )

                if self._startup_future is None:
                    raise LifespanNotStarted("startup")

                if self._startup_future.done():
                    raise LifespanAlreadyCompleted("startup")

                context.state = LifespanState.FAILED

                self._startup_future.set_exception(
                    RuntimeError(
                        event.get(
                            "message",
                            "Application startup failed.",
                        )
                    )
                )

            case LifespanEventType.SHUTDOWN_COMPLETE:

                if context.state is not LifespanState.STOPPING:
                    raise InvalidLifespanState(
                        context.state,
                        event["type"],
                    )

                if self._shutdown_future is None:
                    raise LifespanNotStarted("shutdown")

                if self._shutdown_future.done():
                    raise LifespanAlreadyCompleted("shutdown")

                context.state = LifespanState.STOPPED
                self._shutdown_future.set_result(None)

            case LifespanEventType.SHUTDOWN_FAILED:

                if context.state is not LifespanState.STOPPING:
                    raise InvalidLifespanState(
                        context.state,
                        event["type"],
                    )

                if self._shutdown_future is None:
                    raise LifespanNotStarted("shutdown")

                if self._shutdown_future.done():
                    raise LifespanAlreadyCompleted("shutdown")

                context.state = LifespanState.FAILED

                self._shutdown_future.set_exception(
                    RuntimeError(
                        event.get(
                            "message",
                            "Application shutdown failed.",
                        )
                    )
                )

            case _:
                raise InvalidEvent(event["type"])