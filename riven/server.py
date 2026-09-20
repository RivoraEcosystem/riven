from .protocol_h3 import (
    RivenH3,
    ConnectionInfo,
    HTTP3Stream,
)
from aioquic.asyncio.server import serve , QuicServer
from aioquic.quic.configuration import QuicConfiguration
from aioquic.h3.connection import H3_ALPN
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
from logger.colors import colorize , ANSIColor
import os
import asyncio
from ._config import (
    RivenConfig,
    STARTUP_SHUTDOWN_FAILURE
    )
from typing import Any ,TYPE_CHECKING

import sys
import logging

type LifeSpan = LifeSpanOff|LifeSpanOn

logger = logging.getLogger('riven')

class RivenServer: # server connection manager 
    def __init__(
        self,
        config:RivenConfig,
    ):
        self.config = config
        self.started:bool = False
        self.should_exit:bool = False
        self.force_exit:bool = False

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

        self.server:QuicServer = await serve(
            host=config.host,
            port=config.port,
            configuration=configuration,
            create_protocol=lambda *args, **kwargs:
            RivenH3(
                config=self.config,
                app_state=self.lifespan.state,
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

        self.lifespan:LifeSpan = config.lifespan_class(config)

        startup_message = "Started server process [%d]"
        logger.info(startup_message,processID)

    
        