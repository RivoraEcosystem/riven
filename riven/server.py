from .protocol_h3 import (
    RivenH3,
    ConnectionInfo,
    HTTP3Stream,
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
    InvalidStream,
    InvalidLifespanState,
    LifespanAlreadyCompleted,
    LifespanNotStarted
)

from .lifespan import (
    LifeSpanOn,
    LifeSpanOff,
    LifespanState
)

import asyncio
from ._config import (
    RivenConfig,
    STARTUP_SHUTDOWN_FAILURE
    )
from typing import Any
import sys

type LifeSpan = LifeSpanOff|LifeSpanOn


class RivenServer: # server connection manager 
    def __init__(
        self,
        config:RivenConfig,
    ):
        self.config = config
        self.started:bool = False
        self.should_exit:bool = False
    