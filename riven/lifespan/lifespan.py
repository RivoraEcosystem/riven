from __future__ import annotations

import asyncio
from enum import Enum, auto
from typing import TYPE_CHECKING

from rcp import (
    LifespanScope,
    RCPReceiveEvent,
    RCPSendEvent,
    LifespanEventType,
    LifespanStartupEvent,
    LifespanShutdownEvent
)

if TYPE_CHECKING:
    from ..server import Riven

class LifespanState(Enum):
    INITIAL = auto()
    STARTING = auto()
    STARTED = auto()
    STOPPING = auto()
    STOPPED = auto()
    FAILED = auto()


class LifespanContext:

    def __init__(
        self,
        manager: Riven,
        scope: LifespanScope,
    ) -> None:
        self.manager = manager
        self.scope = scope

        self.queue: asyncio.Queue[RCPReceiveEvent] = asyncio.Queue()
        self.task: asyncio.Task[None] | None = None

        self._state = LifespanState.INITIAL
        self._closed = False

    async def receive(self) -> RCPReceiveEvent | None:
        return await self._pop()

    async def send(
        self,
        event: RCPSendEvent,
    ) -> None:
        await self.manager.handle_lifespan_event(self, event)

    async def _push(
        self,
        event: RCPReceiveEvent,
    ) -> None:
        try:
            await self.queue.put(event)
        except asyncio.QueueShutDown:
            pass

    async def close(self) -> None:
        self._closed = True

        self.queue.shutdown(immediate=True)

        if self.task is not None and not self.task.done():
            self.task.cancel()
    
    async def _pop(self) -> RCPReceiveEvent | None:
        try:
            return await self.queue.get()
        except asyncio.QueueShutDown:
            return None

    async def startup(self) -> None:
        event: LifespanStartupEvent = {"type":LifespanEventType.STARTUP}
        await self._push(event)   

    async def shutdown(self) -> None:
        event: LifespanShutdownEvent = {"type":LifespanEventType.SHUTDOWN} 
        await self._push(event)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def state(self) -> LifespanState:
        return self._state