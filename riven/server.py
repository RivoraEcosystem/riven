from .protocol_h3 import (
    RivenH3,
    ConnectionInfo,
    HTTP3Stream,
)
from aioquic.asyncio.server import serve , QuicServer
from aioquic.quic.configuration import QuicConfiguration
from aioquic.h3.connection import H3_ALPN
from .exceptions.exceptions import (
    RivenException
)
from .lifespan import (
    LifeSpanOn,
    LifeSpanOff,
)
from logger.colors import colorize , ANSIColor
import os
import asyncio
from ._config import (
    RivenConfig,
    STARTUP_SHUTDOWN_FAILURE
    )
from typing import Any ,TYPE_CHECKING , Generator
from types import FrameType

import signal
import threading
import sys
import logging
import contextlib

type LifeSpan = LifeSpanOff|LifeSpanOn

HANDLED_SIGNALS = (
    signal.SIGINT, # UNIX 2 : CTRL + C
    signal.SIGTERM  # UNIX 15 : `kill <pid>`
)

if sys.platform == "win32":
    HANDLED_SIGNALS += (signal.SIGBREAK) # windows signal 21 : CTRL + Break

logger = logging.getLogger('riven')


class RivenState:
    """Shared server state that is avaliable to all protocol instances"""
    def __init__(self) -> None:
        self._total_connections:int = 0
        self.connections: set[RivenH3] = set()

        # RCP/ASGI Application tasks
        self.application_task = set[asyncio.Task[None]] = set()

    @property
    def total_task(self) -> int:
        return len(self.application_task)

    def add_connection(self,connection:RivenH3) -> None:
        """Add connection to server state"""
        self.connections.add(connection)
    
    def remove_connection(self,connection:RivenH3) -> None:
        """Remove connection from server state"""
        self.connections.discard(connection)

class RivenServer: # riven server and lifecycle manager
    def __init__(
        self,
        config:RivenConfig,
    ):
        self.config = config
        self.server_state:RivenState = RivenState()

        self.started:bool = False
        self.should_exit:bool = False
        self.force_exit:bool = False

        self._captured_signals:list[int] = []

        self.server:QuicServer|None = None
        self.lifespan:LifeSpan|None = None

    async def startup(self):
        await self.lifespan.startup()

        config = self.config

        if self.lifespan.should_exit:
            sys.exit(STARTUP_SHUTDOWN_FAILURE)

        configuration = QuicConfiguration(
            alpn_protocols=H3_ALPN,
            is_client=False,
        )

        configuration.load_cert_chain(
            certfile=config.ssl_certfile,
            keyfile=config.ssl_keyfile,
            password=config.ssl_keyfile_password
        )

        self.server = await serve(
            host=config.host,
            port=config.port,
            configuration=configuration,
            create_protocol=lambda *args, **kwargs:
            RivenH3(
                config=self.config,
                app_state=self.lifespan.state.copy(),
                server_state=self.server_state,
                *args,
                **kwargs
            )
        )

        self.started = True

    async def _serve(self):
        processID = os.getpid()
        config = self.config

        if not config.loaded:
            config.load()

        self.lifespan = config.lifespan_class(config)

        startup_message = "Started server process [%d]"
        logger.info(startup_message,processID)


    @contextlib.contextmanager
    def intercept_signals(self) -> Generator[None,None,None]:

        # Only Intercept on main thread
        if threading.current_thread() is not threading.main_thread():
            yield 
            return

        # apply custom hook for signals but store original
        original_handlers = {
            sig: signal.signal(sig,self.handle_exit)
            for sig in HANDLED_SIGNALS
        }
        try:
            yield
        finally:
            # revert to original handlers
            for sig,handler in original_handlers.items():
                signal.signal(sig,handler)

        # Re-raise absorbed signals to parent process
        for captured_signal in reversed(self._captured_signals):
            signal.raise_signal(captured_signal)

    def handle_exit(self,sig:int,frame:FrameType|None) -> None:
        # store captured signal
        self._captured_signals.append(sig)
    
        # double break (ctrl+c) activate force crash state
        if self.should_exit and sig == signal.SIGINT:
            self.force_exit = True
            logger.warning("Force exit signal. Crashing immediately...")
        else:
            self.should_exit = True
            logger.info("Exit signal captured. Initializing graceful wrap-up...")