from rcp import (
    LifespanScope,
    LifespanEventType,
    LifespanStartupEvent,
    LifespanShutdownEvent,
    LifespanStartupFailedEvent,
    LifespanShutdownFailedEvent,
    LifespanStartupCompleteEvent,
    LifespanShutdownCompleteEvent,
    RCPSendEvent,
    RCPReceiveEvent
)
from rcp.rcp import RCPVersions
import asyncio


class Lifespan:
    def __init__(self):
        pass
        