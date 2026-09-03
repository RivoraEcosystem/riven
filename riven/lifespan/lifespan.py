from __future__ import annotations

import asyncio
from enum import Enum, auto
from typing import TYPE_CHECKING , Any

from rcp import (
    LifespanScope,
    RCPReceiveEvent,
    RCPSendEvent,
    LifespanEventType,
    LifespanStartupEvent,
    LifespanShutdownEvent,
    RCPApplication,
    ScopeType,
    RCPVersions,
    LifespanStartupCompleteEvent,
    LifespanStartupFailedEvent,
    LifespanShutdownCompleteEvent,
    LifespanShutdownFailedEvent
)

from ..exceptions.exceptions import(
    InvalidLifespanState,
    LifespanAlreadyCompleted,
    LifespanNotStarted,
    InvalidEvent
)
import logging

if TYPE_CHECKING:
    from ..server import Riven

class LifespanState(Enum):
    INITIAL = auto()
    STARTING = auto()
    STARTED = auto()
    STOPPING = auto()
    STOPPED = auto()
    FAILED = auto()

type LifespanSendEvent = LifespanStartupCompleteEvent|LifespanStartupFailedEvent|LifespanShutdownCompleteEvent|LifespanShutdownFailedEvent


class LifeSpan:

    def __init__(
        self,
        manager: Riven,
    ) -> None:
        self.manager = manager

        self.queue: asyncio.Queue[RCPReceiveEvent] = asyncio.Queue()
        self.task: asyncio.Task[None] | None = None

        self._state = LifespanState.INITIAL
        self._closed = False

        self.startup_event = asyncio.Event()
        self.shutdown_event = asyncio.Event()

        self.should_exit = False
        self.error_occurred = False
        self.shutdown_failed = False
        self.startup_failed = False
        self.logger = logging.getLogger('riven.lifecycle')

    async def receive(self) -> RCPReceiveEvent | None:
        return await self._pop()

    async def _push(
        self,
        event: RCPReceiveEvent,
    ) -> None:
        try:
            await self.queue.put(event)
        except asyncio.QueueShutDown:
            pass

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        self.queue.shutdown(immediate=True)
    
    async def _pop(self) -> RCPReceiveEvent | None:
        try:
            return await self.queue.get()
        except asyncio.QueueShutDown:
            return None

    async def startup(self) -> None:
        self._state = LifespanState.STARTING
        event: LifespanStartupEvent = {"type":LifespanEventType.STARTUP}
        await self._push(event)   
        await self.startup_event.wait()

        if self.startup_failed or self.error_occurred:
            self.logger.error("Application startup failed. Exiting.")
            self.should_exit = True

    async def shutdown(self) -> None:
        self._state = LifespanState.STOPPING
        event: LifespanShutdownEvent = {"type":LifespanEventType.SHUTDOWN} 
        await self._push(event)
        await self.shutdown_event.wait()

        if self.shutdown_failed or self.error_occurred:
            self.logger.error("Application shutdown failed. Exiting.")
            self.should_exit = True

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def state(self) -> LifespanState:
        return self._state

    async def main(
        self,
        app:RCPApplication,
        state: dict[str, Any]|None,
        extensions: dict[str, dict[object, object]]|None
    ) -> None:

        try:
            scope: LifespanScope = {
                "type": ScopeType.LIFESPAN,
                "rcp": {"version": RCPVersions.VERSION_1},
            }

            if state is not None:
                scope['state'] = state

            if extensions is not None:
                scope["extensions"] = extensions
            

            await app(scope, self.receive, self.send)

        except Exception as exc:
            self.error_occurred = True

            if self.startup_failed or self.shutdown_failed:
                return

            msg = "Exception in 'lifespan' protocol\n"
            self.logger.error(msg, exc_info=exc)

        finally:
            self.startup_event.set()
            self.shutdown_event.set()
            await self.close()

    async def send(self,event: LifespanSendEvent) -> None:

        match event["type"]:

            case LifespanEventType.STARTUP_COMPLETE:

                if self._state is not LifespanState.STARTING:
                    raise InvalidLifespanState(
                        self._state,
                        event["type"],
                    )

                self._state = LifespanState.STARTED
                self.startup_event.set()

            case LifespanEventType.STARTUP_FAILED:

                if self._state is not LifespanState.STARTING:
                    raise InvalidLifespanState(
                        self._state,
                        event["type"],
                    )

                if event.get('message'):
                    self.logger.error(event["message"])

                self._state = LifespanState.FAILED
                self.startup_failed = True
                self.should_exit = True
                self.startup_event.set()

            case LifespanEventType.SHUTDOWN_COMPLETE:

                if self._state is not LifespanState.STOPPING:
                    raise InvalidLifespanState(
                        self._state,
                        event["type"],
                    )

                self._state = LifespanState.STOPPED
                self.shutdown_event.set()

            case LifespanEventType.SHUTDOWN_FAILED:

                if self._state is not LifespanState.STOPPING:
                    raise InvalidLifespanState(
                        self._state,
                        event["type"],
                    )

                if event.get('message'):
                    self.logger.error(event["message"])

                self._state = LifespanState.FAILED
                self.shutdown_failed = True
                self.should_exit = True
                self.shutdown_event.set()

            case _:
                raise InvalidEvent(event["type"])